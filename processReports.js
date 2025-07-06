const admin = require("firebase-admin");

// Load credentials from env
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

// How old (in seconds) until stuff gets removed
const MAX_AGE = 48 * 60 * 60;

// Distance in km to treat two reports as “same place”
const MERGE_RADIUS = 0.1;

// Get distance between 2 points
function haversine(lat1, lon1, lat2, lon2) {
  const toRad = x => x * Math.PI / 180;
  const R = 6371;
  const dLat = toRad(lat2 - lat1);
  const dLon = toRad(lon2 - lon1);
  const a =
    Math.sin(dLat / 2) ** 2 +
    Math.cos(toRad(lat1)) * Math.cos(toRad(lat2)) *
    Math.sin(dLon / 2) ** 2;
  const c = 2 * Math.atan2(Math.sqrt(a), Math.sqrt(1 - a));
  return R * c;
}

(async () => {
  const now = Math.floor(Date.now() / 1000);
  let deleteCount = 0;
  let mergeCount = 0;

  try {
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

    const mergedTimestamps = new Set(
      Object.values(mergedData)
        .filter(m => m && typeof m.timestamp === 'number')
        .map(m => m.timestamp)
    );

    // 🗑️ Clean up old /reports
    for (const [id, rpt] of Object.entries(liveData)) {
      if (!rpt?.timestamp || typeof rpt.timestamp !== "number") continue;
      if (now - rpt.timestamp > MAX_AGE && !mergedTimestamps.has(rpt.timestamp)) {
        updates[`/reports/${id}`] = null;
        deleteCount++;
      }
    }

    // 🗑️ Clean up old /reports_trash
    for (const [id, rpt] of Object.entries(trashData)) {
      if (!rpt?.timestamp || typeof rpt.timestamp !== "number") continue;
      if (now - rpt.timestamp > MAX_AGE) {
        updates[`/reports_trash/${id}`] = null;
        deleteCount++;
      }
    }

    // 🗑️ Clear expired lastReportTimestamps in /users
    for (const [uid, user] of Object.entries(usersData)) {
      if (!user?.lastReportTimestamp || typeof user.lastReportTimestamp !== "number") continue;
      if (now - user.lastReportTimestamp > MAX_AGE) {
        updates[`/users/${uid}/lastReportTimestamp`] = null;
        deleteCount++;
      }
    }

    // 🗑️ Remove expired /merged_reports
    for (const [id, rpt] of Object.entries(mergedData)) {
      if (!rpt?.timestamp || typeof rpt.timestamp !== "number") continue;
      if (now - rpt.timestamp > MAX_AGE) {
        updates[`/merged_reports/${id}`] = null;
        deleteCount++;
      }
    }

    // 🧹 Deduplicate merged_reports based on same lat/lng
    const seenCoords = new Map(); // "lat,lng" -> {id, descLength}
    for (const [id, rpt] of Object.entries(mergedData)) {
      if (!rpt?.latitude || !rpt?.longitude) continue;
      const key = `${rpt.latitude},${rpt.longitude}`;
      const descLength = rpt.description?.length || 0;

      if (seenCoords.has(key)) {
        const existing = seenCoords.get(key);
        if (descLength > existing.descLength) {
          updates[`/merged_reports/${existing.id}`] = null;
          seenCoords.set(key, { id, descLength });
        } else {
          updates[`/merged_reports/${id}`] = null;
        }
        deleteCount++;
      } else {
        seenCoords.set(key, { id, descLength });
      }
    }

    // 🧹 Clean up duplicates between live and merged
    for (const [id, rpt] of Object.entries(liveData)) {
      if (!rpt?.latitude || !rpt?.longitude) continue;
      for (const [mergedId, mergedRpt] of Object.entries(mergedData)) {
        if (
          mergedRpt &&
          mergedRpt.latitude === rpt.latitude &&
          mergedRpt.longitude === rpt.longitude
        ) {
          const lenLive = rpt.description?.length || 0;
          const lenMerged = mergedRpt.description?.length || 0;

          if (lenLive < lenMerged) {
            updates[`/reports/${id}`] = null;
            deleteCount++;
          } else if (lenLive > lenMerged) {
            updates[`/merged_reports/${mergedId}`] = null;
            deleteCount++;
          }
        }
      }
    }

    // 🔁 Put any valid merged reports back into /reports
    for (const [id, rpt] of Object.entries(mergedData)) {
      if (!rpt?.timestamp || typeof rpt.timestamp !== "number") continue;
      if (!liveData[id] && now - rpt.timestamp <= MAX_AGE) {
        updates[`/reports/${id}`] = rpt;
      }
    }

    // 🔄 Merge nearby reports in /reports
    const processed = new Set();
    const entries = Object.entries(liveData);

    for (let i = 0; i < entries.length; i++) {
      const [idA, rA] = entries[i];
      if (
        processed.has(idA) ||
        !rA?.type ||
        typeof rA.latitude !== "number" ||
        typeof rA.longitude !== "number" ||
        typeof rA.timestamp !== "number" ||
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
      const mergedObj = {
        description: mergedDesc,
        icon: latestRpt.icon,
        latitude: latestRpt.latitude,
        longitude: latestRpt.longitude,
        timestamp: maxTs,
        type: latestRpt.type,
        usersubmitting: "0", // this is a system-merged one
      };

      const clusterIds = new Set(cluster.map(([rid]) => rid));

      // ✅ only block duplicates that aren't part of this merge
      const isDuplicate = Object.entries(liveData).some(([existingId, existing]) =>
        existing &&
        !clusterIds.has(existingId) &&
        existing.latitude === mergedObj.latitude &&
        existing.longitude === mergedObj.longitude
      );

      if (!isDuplicate) {
        updates[`/reports/${newKey}`] = mergedObj;
      } else {
        console.log(`⚠️ Skipped merged report at (${mergedObj.latitude}, ${mergedObj.longitude}) due to existing report.`);
      }

      updates[`/merged_reports/${newKey}`] = mergedObj;

      for (const [oldId, oldRpt] of cluster) {
        updates[`/reports_trash/${oldId}`] = oldRpt;
        updates[`/reports/${oldId}`] = null;
      }
    }

    await db.ref().update(updates);
    console.log(`✅ Deleted ${deleteCount} items, merged ${mergeCount} clusters.`);
    console.log("🎉 Cleanup and merge succeeded.");
  } catch (err) {
    console.error("❌ Error in cleanup and merge:", err);
  }

  process.exit(0);
})();
