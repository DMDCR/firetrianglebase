const admin = require("firebase-admin");

const serviceAccount = JSON.parse(process.env.FIREBASE_SERVICE_ACCOUNT);

admin.initializeApp({
  credential: admin.credential.cert(serviceAccount),
  databaseURL: process.env.FIREBASE_DB_URL,
});

const db = admin.database();
const liveRef = db.ref("reports");
const mergedRef = db.ref("merged_reports");
const trashRef = db.ref("reports_trash");
const usersRef = db.ref("users");

const MAX_AGE = 48 * 60 * 60;
const MERGE_RADIUS = 0.1;

function haversine(lat1, lon1, lat2, lon2) {
  const toRad = x => x * Math.PI / 180;
  const R = 6371;
  const dLat = toRad(lat2 - lat1);
  const dLon = toRad(lon2 - lon1);
  const a = Math.sin(dLat / 2) ** 2 +
            Math.cos(toRad(lat1)) * Math.cos(toRad(lat2)) *
            Math.sin(dLon / 2) ** 2;
  const c = 2 * Math.atan2(Math.sqrt(a), Math.sqrt(1 - a));
  return R * c;
}

(async () => {
  const now = Math.floor(Date.now() / 1000);
  let deleteCount = 0;
  let mergeCount = 0;
  let duplicateSkips = 0;

  try {
    console.log("🔄 Fetching database snapshots...");
    const [liveSnap, trashSnap, usersSnap, mergedSnap] = await Promise.all([
      liveRef.once("value"),
      trashRef.once("value"),
      usersRef.once("value"),
      mergedRef.once("value"),
    ]);

    const liveData = liveSnap.val() || {};
    const trashData = trashSnap.val() || {};
    const usersData = usersSnap.val() || {};
    const mergedData = mergedSnap.val() || {};
    const updates = {};

    console.log("🧹 Starting cleanup...");

    const mergedTimestamps = new Set(
      Object.values(mergedData)
        .filter(m => m?.timestamp)
        .map(m => m.timestamp)
    );

    for (const [id, rpt] of Object.entries(liveData)) {
      if (!rpt?.timestamp) continue;
      if (now - rpt.timestamp > MAX_AGE && !mergedTimestamps.has(rpt.timestamp)) {
        updates[`/reports/${id}`] = null;
        deleteCount++;
        console.log(`🗑️ Deleted old report ${id}`);
      }
    }

    for (const [id, rpt] of Object.entries(trashData)) {
      if (!rpt?.timestamp) continue;
      if (now - rpt.timestamp > MAX_AGE) {
        updates[`/reports_trash/${id}`] = null;
        deleteCount++;
        console.log(`🗑️ Deleted old trash ${id}`);
      }
    }

    for (const [uid, user] of Object.entries(usersData)) {
      if (!user?.lastReportTimestamp) continue;
      if (now - user.lastReportTimestamp > MAX_AGE) {
        updates[`/users/${uid}/lastReportTimestamp`] = null;
        deleteCount++;
        console.log(`🗑️ Cleared stale user timestamp for ${uid}`);
      }
    }

    for (const [id, rpt] of Object.entries(mergedData)) {
      if (!rpt?.timestamp) continue;
      if (now - rpt.timestamp > MAX_AGE) {
        updates[`/merged_reports/${id}`] = null;
        deleteCount++;
        console.log(`🗑️ Deleted old merged report ${id}`);
      }
    }

    const seenCoords = new Map();
    for (const [id, rpt] of Object.entries(mergedData)) {
      if (!rpt?.latitude || !rpt?.longitude) continue;
      const key = `${rpt.latitude},${rpt.longitude}`;
      const descLength = rpt.description?.length || 0;

      if (seenCoords.has(key)) {
        const existing = seenCoords.get(key);
        if (descLength > existing.descLength) {
          updates[`/merged_reports/${existing.id}`] = null;
          seenCoords.set(key, { id, descLength });
          console.log(`🗑️ Removed shorter duplicate from merged_reports`);
        } else {
          updates[`/merged_reports/${id}`] = null;
          console.log(`🗑️ Removed shorter duplicate from merged_reports`);
        }
        deleteCount++;
      } else {
        seenCoords.set(key, { id, descLength });
      }
    }

    for (const [id, rpt] of Object.entries(liveData)) {
      if (!rpt?.latitude || !rpt?.longitude) continue;

      for (const [mid, mrpt] of Object.entries(mergedData)) {
        if (
          mrpt &&
          mrpt.latitude === rpt.latitude &&
          mrpt.longitude === rpt.longitude
        ) {
          const len1 = rpt.description?.length || 0;
          const len2 = mrpt.description?.length || 0;

          if (len1 < len2) {
            updates[`/reports/${id}`] = null;
            deleteCount++;
            console.log(`🧼 Deleted shorter duplicate in reports`);
          } else {
            updates[`/merged_reports/${mid}`] = null;
            deleteCount++;
            console.log(`🧼 Deleted shorter duplicate in merged_reports`);
          }
        }
      }
    }

    for (const [id, rpt] of Object.entries(mergedData)) {
      if (!rpt?.timestamp) continue;
      if (!liveData[id] && now - rpt.timestamp <= MAX_AGE) {
        updates[`/reports/${id}`] = rpt;
        console.log(`♻️ Restored valid merged report to /reports`);
      }
    }

    console.log("🔍 Building coordinate map for deduplication...");
    const latLonInUse = new Set(
      Object.entries(liveData)
        .filter(([id]) => !updates[`/reports/${id}`])
        .map(([, rpt]) => `${rpt.latitude},${rpt.longitude}`)
    );

    const processed = new Set();
    const entries = Object.entries(liveData);

    for (let i = 0; i < entries.length; i++) {
      const [idA, rA] = entries[i];
      if (
        processed.has(idA) ||
        !rA?.type ||
        !rA.latitude ||
        !rA.longitude ||
        !rA.timestamp ||
        !rA.icon ||
        !rA.usersubmitting
      ) continue;

      const cluster = [[idA, rA]];
      processed.add(idA);

      for (let j = i + 1; j < entries.length; j++) {
        const [idB, rB] = entries[j];
        if (processed.has(idB) || !rB || rB.type !== rA.type) continue;

        const dist = haversine(rA.latitude, rA.longitude, rB.latitude, rB.longitude);
        if (dist <= MERGE_RADIUS) {
          cluster.push([idB, rB]);
          processed.add(idB);
        }
      }

      if (cluster.length < 2) continue;

      mergeCount++;

      const mergedDesc = cluster.map(([, r]) => r.description || "").join(", ");
      const maxTs = Math.max(...cluster.map(([, r]) => r.timestamp));
      const latestRpt = cluster.find(([, r]) => r.timestamp === maxTs)?.[1] || cluster[0][1];

      const newKey = liveRef.push().key;
      const lat = latestRpt.latitude;
      const lon = latestRpt.longitude;
      const coordKey = `${lat},${lon}`;

      const mergedObj = {
        description: mergedDesc,
        icon: latestRpt.icon,
        latitude: lat,
        longitude: lon,
        timestamp: maxTs,
        type: latestRpt.type,
        usersubmitting: "0",
      };

      if (!latLonInUse.has(coordKey)) {
        updates[`/reports/${newKey}`] = mergedObj;
        latLonInUse.add(coordKey);
        console.log(`✅ Merged cluster added to /reports`);
      } else {
        duplicateSkips++;
        console.log(`⚠️ Skipped duplicate merged report at ${coordKey}`);
      }

      updates[`/merged_reports/${newKey}`] = mergedObj;

      for (const [rid, rpt] of cluster) {
        updates[`/reports_trash/${rid}`] = rpt;
        updates[`/reports/${rid}`] = null;
        console.log(`🗃️ Archived ${rid} to trash and removed from reports`);
      }
    }

    console.log("🚀 Applying updates to Firebase...");
    await db.ref().update(updates);

    console.log(`\n✅ Done!`);
    console.log(`• Deleted: ${deleteCount}`);
    console.log(`• Merged clusters: ${mergeCount}`);
    console.log(`• Skipped (lat/lon duplicates): ${duplicateSkips}`);
    console.log(`🎉 Cleanup and merge finished with no exposed data.`);
  } catch (err) {
    console.error("❌ Script error:", err.message);
  }

  process.exit(0);
})();
