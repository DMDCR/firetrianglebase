const admin = require("firebase-admin");

const serviceAccount = JSON.parse(process.env.FIREBASE_SERVICE_ACCOUNT);

admin.initializeApp({
  credential: admin.credential.cert(serviceAccount),
  databaseURL: process.env.FIREBASE_DB_URL,
});

const db = admin.database();
const liveRef = db.ref("reports");
const trashRef = db.ref("reports_trash");

const MAX_AGE = 48 * 60 * 60; // 48 hours in seconds

const now = Math.floor(Date.now() / 1000); // Get current timestamp in seconds

(async () => {
  try {
    // Fetch reports from live and trash collections
    const [liveSnap, trashSnap] = await Promise.all([
      liveRef.once("value"),
      trashRef.once("value"),
    ]);

    const liveData = liveSnap.exists() ? liveSnap.val() : {};
    const trashData = trashSnap.exists() ? trashSnap.val() : {};

    // Delete reports older than 48 hours from live collection
    const liveDeletes = {};
    for (const id in liveData) {
      const report = liveData[id];
      if (report.timestamp && now - report.timestamp > MAX_AGE) {
        liveDeletes[`/reports/${id}`] = null; // Mark for deletion
      }
    }

    // Delete reports older than 48 hours from trash collection
    const trashDeletes = {};
    for (const id in trashData) {
      const report = trashData[id];
      if (report.timestamp && now - report.timestamp > MAX_AGE) {
        trashDeletes[`/reports_trash/${id}`] = null; // Mark for deletion
      }
    }

    // Write the deletion updates back to Firebase
    await db.ref().update({
      ...liveDeletes,
      ...trashDeletes,
    });

    console.log("✅ Successfully deleted reports older than 48 hours.");
    process.exit(0);
  } catch (err) {
    console.error("❌ Failed to clean reports:", err);
    process.exit(1);
  }
})();
