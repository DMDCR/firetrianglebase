const admin = require("firebase-admin");

const serviceAccount = JSON.parse(process.env.FIREBASE_SERVICE_ACCOUNT);

admin.initializeApp({
  credential: admin.credential.cert(serviceAccount),
  databaseURL: process.env.FIREBASE_DB_URL,
});

const db = admin.database();
const liveRef = db.ref("reports");
const trashRef = db.ref("reports_trash");
const usersRef = db.ref("users");

const MAX_AGE = 48 * 60 * 60; // 48 hours in seconds

const now = Math.floor(Date.now() / 1000); // Get current timestamp in seconds

(async () => {
  try {
    // Fetch reports from live and trash collections
    const [liveSnap, trashSnap, usersSnap] = await Promise.all([
      liveRef.once("value"),
      trashRef.once("value"),
      usersRef.once("value"), // Fetch users' data
    ]);

    const liveData = liveSnap.exists() ? liveSnap.val() : {};
    const trashData = trashSnap.exists() ? trashSnap.val() : {};
    const usersData = usersSnap.exists() ? usersSnap.val() : {};

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

    // Clear the `lastReportTimestamp` field for users whose last report is older than 48 hours
    const userUpdates = {};
    for (const userId in usersData) {
      const user = usersData[userId];
      const lastReportTimestamp = user.lastReportTimestamp;

      // Check if last report timestamp is older than 48 hours
      if (lastReportTimestamp && now - lastReportTimestamp > MAX_AGE) {
        userUpdates[`/users/${userId}/lastReportTimestamp`] = null; // Clear the lastReportTimestamp field
      }
    }

    // Write the deletion updates and user updates back to Firebase
    await db.ref().update({
      ...liveDeletes,
      ...trashDeletes,
      ...userUpdates, // Apply the user updates
    });

    console.log("✅ Successfully deleted reports older than 48 hours and cleared lastReportTimestamp.");
    process.exit(0);
  } catch (err) {
    console.error("❌ Failed to clean reports and update users:", err);
    process.exit(1);
  }
})();
