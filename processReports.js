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
  const a = Math.sin(dLat / 2) ** 2 + Math.cos(toRad(lat1)) * Math.cos(toRad(lat2)) * Math.sin(dLon / 2) ** 2;
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

    const mergedTimestamps = new Set(Object.values(mergedData).map(m => m.timestamp));

    // Check and delete old live reports
    Object.entries(liveData).forEach(([id, rpt]) => {
      if (rpt.timestamp && now - rpt.timestamp > MAX_AGE && !mergedTimestamps.has(rpt.timestamp)) {
        updates[`/reports/${id}`] = null;
        deleteCount++;
      }
    });

    // Check and delete old trash reports
    Object.entries(trashData).forEach(([id, rpt]) => {
      if (rpt.timestamp && now - rpt.timestamp > MAX_AGE) {
        updates[`/reports_trash/${id}`] = null;
        deleteCount++;
      }
    });

    // Check and delete old user report timestamps
    Object.entries(usersData).forEach(([uid, usr]) => {
      if (usr.lastReportTimestamp && now - usr.lastReportTimestamp > MAX_AGE) {
        updates[`/users/${uid}/lastReportTimestamp`] = null;
        deleteCount++;
      }
    });

    // Check and delete old merged reports that are over 48 hours old
    Object.entries(mergedData).forEach(([id, rpt]) => {
      if (rpt.timestamp && now - rpt.timestamp > MAX_AGE) {
        updates[`/merged_reports/${id}`] = null;
        deleteCount++;
      }
    });

    // Check for identical reports in merged_reports and delete the shorter description
    const seen = new Set();
    Object.entries(mergedData).forEach(([id, rpt]) => {
      const key = `${rpt.latitude},${rpt.longitude}`;
      if (seen.has(key)) {
        updates[`/merged_reports/${id}`] = null;
        deleteCount++;
      } else {
        seen.add(key);
      }
    });

    // Check for identical reports in reports/ and delete the shorter description
    Object.entries(liveData).forEach(([id, rpt]) => {
      const key = `${rpt.latitude},${rpt.longitude}`;
      Object.entries(mergedData).forEach(([mergedId, mergedRpt]) => {
        if (key === `${mergedRpt.latitude},${mergedRpt.longitude}`) {
          const currentDescLength = rpt.description ? rpt.description.length : 0;
          const mergedDescLength = mergedRpt.description ? mergedRpt.description.length : 0;

          // If the description in liveData is shorter, delete it
          if (currentDescLength < mergedDescLength) {
            updates[`/reports/${id}`] = null;
            deleteCount++;
          } else if (currentDescLength > mergedDescLength) {
            // If the description in mergedData is shorter, delete it
            updates[`/merged_reports/${mergedId}`] = null;
            deleteCount++;
          }
        }
      });
    });

    // Copy over merged reports that are missing from live reports
    Object.entries(mergedData).forEach(([id, rpt]) => {
      if (!liveData[id] && rpt.timestamp && now - rpt.timestamp <= MAX_AGE) {
        updates[`/reports/${id}`] = rpt;
      }
    });

    const processed = new Set();
    const entries = Object.entries(liveData);
    for (let i = 0; i < entries.length; i++) {
      const [idA, rA] = entries[i];
      if (processed.has(idA) || !rA.type || !rA.latitude || !rA.longitude || !rA.timestamp || !rA.icon || !rA.usersubmitting) continue;

      const cluster = [[idA, rA]];
      processed.add(idA);

      for (let j = i + 1; j < entries.length; j++) {
        const [idB, rB] = entries[j];
        if (processed.has(idB) || rB.type !== rA.type) continue;

        const dist = haversine(rA.latitude, rA.longitude, rB.latitude, rB.longitude);
        if (dist <= MERGE_RADIUS) {
          cluster.push([idB, rB]);
          processed.add(idB);
        }
      }

      if (cluster.length < 2) continue;

      mergeCount++;

      const mergedDesc = cluster.map(([, rpt]) => rpt.description || "").filter(d => d).join(",");
      const timestamps = cluster.map(([, rpt]) => rpt.timestamp || 0);
      const maxTs = Math.max(...timestamps);
      const latestRpt = cluster.find(([, rpt]) => rpt.timestamp === maxTs)[1];
      const icon = latestRpt.icon || "";
      const latitude = latestRpt.latitude;
      const longitude = latestRpt.longitude;
      const type = latestRpt.type;

      const newKey = liveRef.push().key;
      const mergedObj = {
        description: mergedDesc,
        icon,
        latitude,
        longitude,
        timestamp: maxTs,
        type,
        usersubmitting: "0",
      };

      updates[`/reports/${newKey}`] = mergedObj;
      updates[`/merged_reports/${newKey}`] = mergedObj;

      cluster.forEach(([rid, rpt]) => {
        updates[`/reports_trash/${rid}`] = rpt;
        updates[`/reports/${rid}`] = null;
      });
    }

    await db.ref().update(updates);
    console.log(`✅ Deleted ${deleteCount} items, merged ${mergeCount} clusters.`);
    console.log("🎉 Cleanup and merge succeeded.");
  } catch (err) {
    console.error("Error in cleanup and merge:", err);
  }

  process.exit(0);
})();
