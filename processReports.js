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

    // Cleanup old reports and trash
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

    // Build a coordinate map of live reports
    const latLonInUse = new Set(
      Object.entries(liveData)
        .filter(([id]) => !updates[`/reports/${id}`]) // Only live reports not being deleted
        .map(([, rpt]) => `${rpt.latitude},${rpt.longitude}`)
    );

    // Process the merged reports for possible movement to live reports
    for (const [id, rpt] of Object.entries(mergedData)) {
      if (!rpt?.latitude || !rpt?.longitude || !rpt?.timestamp) continue;

      const coordKey = `${rpt.latitude},${rpt.longitude}`;

      // Check if this coordinate already exists in /reports
      if (!latLonInUse.has(coordKey)) {
        const newKey = liveRef.push().key;
        const mergedObj = {
          description: rpt.description,
          icon: rpt.icon,
          latitude: rpt.latitude,
          longitude: rpt.longitude,
          timestamp: rpt.timestamp,
          type: rpt.type,
          usersubmitting: "0",
        };

        // Move merged report to /reports
        updates[`/reports/${newKey}`] = mergedObj;
        latLonInUse.add(coordKey);
        mergeCount++;

        // Remove merged report from merged_reports
        updates[`/merged_reports/${id}`] = null;
        console.log(`✅ Moved merged report to /reports (${coordKey})`);
      } else {
        duplicateSkips++;
        console.log(`⚠️ Skipped duplicate merged report at ${coordKey}`);
      }
    }

    console.log("🚀 Applying updates to Firebase...");
    await db.ref().update(updates);

    console.log(`\n✅ Done!`);
    console.log(`• Deleted: ${deleteCount}`);
    console.log(`• Merged clusters moved to /reports: ${mergeCount}`);
    console.log(`• Skipped (duplicate lat/lon): ${duplicateSkips}`);
    console.log(`🎉 Cleanup and merge finished with no exposed data.`);
  } catch (err) {
    console.error("❌ Script error:", err.message);
  }

  process.exit(0);
})();
