const admin = require("firebase-admin");

const serviceAccount = JSON.parse(process.env.FIREBASE_SERVICE_ACCOUNT);

admin.initializeApp({
  credential: admin.credential.cert(serviceAccount),
  databaseURL: process.env.FIREBASE_DB_URL
});

const db = admin.database();
const liveRef = db.ref("reports");
const trashRef = db.ref("reports_trash");

const PRECISION = 0.005;
const TIME_WINDOW = 30 * 60;
const MAX_AGE = 48 * 60 * 60; // 48 hours in seconds

const now = Math.floor(Date.now() / 1000);

function norm(val) {
  return Math.round(val / PRECISION) * PRECISION;
}

function clusterKey(r) {
  return [
    r.type,
    norm(r.latitude),
    norm(r.longitude),
    Math.floor(r.timestamp / TIME_WINDOW)
  ].join("|");
}

(async () => {
  try {
    const [liveSnap, trashSnap] = await Promise.all([
      liveRef.once("value"),
      trashRef.once("value")
    ]);

    const liveData = liveSnap.exists() ? liveSnap.val() : {};
    const trashData = trashSnap.exists() ? trashSnap.val() : {};

    // purge expired trash
    const trashDeletes = {};
    for (const id in trashData) {
      const r = trashData[id];
      if (r.timestamp && now - r.timestamp > MAX_AGE) {
        trashDeletes[`/reports_trash/${id}`] = null;
      }
    }

    // group new candidates (skip flagged)
    const sets = {};
    const toMove = [];

    for (const id in liveData) {
      const r = liveData[id];
      if (!r || r.flagged || !r.latitude || !r.longitude || !r.timestamp || !r.type) continue;

      const key = clusterKey(r);
      if (!sets[key]) sets[key] = [];
      sets[key].push({ ...r, _id: id });
    }

    const existingKeys = new Set(
      Object.values(trashData)
        .filter(t => t.ref && Array.isArray(t.ref))
        .map(clusterKey)
    );

    const newReports = {};

    for (const key in sets) {
      const group = sets[key];
      if (group.length < 2) continue;
      if (existingKeys.has(key)) continue; // skip duplicate cluster

      const latAvg = group.reduce((sum, r) => sum + r.latitude, 0) / group.length;
      const lngAvg = group.reduce((sum, r) => sum + r.longitude, 0) / group.length;
      const timestamps = group.map(r => r.timestamp);
      const mid = Math.floor((Math.min(...timestamps) + Math.max(...timestamps)) / 2);

      const newData = {
        type: group[0].type,
        latitude: +latAvg.toFixed(5),
        longitude: +lngAvg.toFixed(5),
        description: `[${group.length}] user confirmations`,
        icon: group[0].icon || "mappin",
        flagged: true,
        timestamp: mid,
        ref: group.map(r => r._id)
      };

      const newId = db.ref("reports").push().key;
      newReports[`/reports/${newId}`] = newData;

      group.forEach(r => {
        toMove.push(r);
      });
    }

    // move used reports to trash
    const trashPayload = {};
    const cleanupLive = {};
    toMove.forEach(r => {
      trashPayload[`/reports_trash/${r._id}`] = r;
      cleanupLive[`/reports/${r._id}`] = null;
    });

    // write everything
    await db.ref().update({
      ...newReports,
      ...trashPayload,
      ...cleanupLive,
      ...trashDeletes
    });

    process.exit(0);
  } catch (err) {
    process.exit(1);
  }
})();
