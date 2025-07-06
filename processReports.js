const admin = require("firebase-admin");

// Load credentials from environment variables
const serviceAccount = JSON.parse(process.env.FIREBASE_SERVICE_ACCOUNT);

// Initialize Firebase
admin.initializeApp({
  credential: admin.credential.cert(serviceAccount),
  databaseURL: process.env.FIREBASE_DB_URL,
});

const db = admin.database();
const liveRef = db.ref("reports");
const mergedRef = db.ref("merged_reports");
const trashRef = db.ref("reports_trash");
const usersRef = db.ref("users");

// Report expires after 48 hours (in seconds)
const MAX_AGE = 48 * 60 * 60;

// Distance in km to consider two reports the same
const MERGE_RADIUS = 0.1;

// Calculate distance between two GPS coordinates
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
  const now = Math.floor(Date.now() / 1000); // current time in seconds
  let deleteCount = 0;
  let mergeCount = 0;

  try {
    // Get all relevant data from the database
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

    // Clean up old reports in /reports (live)
    for (const [id, rpt] of Object.entries(liveData)) {
      if (!rpt?.timestamp || typeof rpt.timestamp !== "number") continue;
      if (now - rpt.timestamp > MAX_AGE && !mergedTimestamps.has(rpt.timestamp)) {
        updates[`/reports/${id}`] = null;
        deleteCount++;
      }
    }

    // Clean up old trash
    for (const [id, rpt] of Object.entries(trashData)) {
      if (!rpt?.timestamp || typeof rpt.timestamp !== "number") continue;
      if (now - rpt.timestamp > MAX_AGE) {
        updates[`/reports_trash/${id}`] = null;
        deleteCount++;
      }
    }

    // Clear out old timestamps under /users
    for (const [uid, user] of Object.entries(usersData)) {
      if (!user?.lastReportTimestamp || typeof user.lastReportTimestamp !== "number") continue;
      if (now - user.lastReportTimestamp > MAX_AGE) {
        updates[`/users/${uid}/lastReportTimestamp`] = null;
        deleteCount++;
      }
    }

    // Clean up merged reports that are too old
    for (const [id, rpt] of Object.entries(mergedData)) {
      if (!rpt?.timestamp || typeof rpt.timestamp !== "number") continue;
      if (now - rpt.timestamp > MAX_AGE) {
        updates[`/merged_reports/${id}`] = null;
        deleteCount++;
      }
    }

    // Remove duplicates in /merged_reports — keep the one with longer description
    const seenCoords = new Map(); // key = lat,lng
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

    // Check if a report exists in both /reports and /merged_reports
    // Keep the longer description
    for (const [id, rpt] of Object.entries(liveData)) {
      if (!rpt?.latitude || !rpt?.longitude) continue;
      const key = `${rpt.latitude},${rpt.longitude}`;

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

    // Restore merged reports back to /reports if they were removed but still valid
    for (const [id, rpt] of Object.entries(mergedData)) {
      if (!rpt?.timestamp || typeof rpt.timestamp !== "number") continue;
      if (!liveData[id] && now - rpt.timestamp <= MAX_AGE) {
        updates[`/reports/${id}`] = rpt;
      }
    }

    // Find clusters in /reports that are close together and merge them
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

      if (cluster.length < 2) continue; // skip if nothing to merge

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
        usersubmitting: "0", // mark as merged/system-created
      };

      // 🛡️ prevent publishing duplicates to /reports
      const isDuplicate = Object.values(liveData).some(existing =>
        existing &&
        existing.latitude === mergedObj.latitude &&
        existing.longitude === mergedObj.longitude
      );

      if (!isDuplicate) {
        updates[`/reports/${newKey}`] = mergedObj;
      }

      updates[`/merged_reports/${newKey}`] = mergedObj;

      // Move all clustered reports to trash and delete them from /reports
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
