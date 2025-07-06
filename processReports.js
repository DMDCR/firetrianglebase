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
  try {
    const [liveSnap, trashSnap, usersSnap] = await Promise.all([
      liveRef.once("value"),
      trashRef.once("value"),
      usersRef.once("value"),
    ]);

    const liveData = liveSnap.val() || {};
    const trashData = trashSnap.val() || {};
    const usersData = usersSnap.val() || {};
    const updates = {};

    Object.entries(liveData).forEach(([id, rpt]) => {
      if (rpt.timestamp && now - rpt.timestamp > MAX_AGE) updates[`/reports/${id}`] = null;
    });
    Object.entries(trashData).forEach(([id, rpt]) => {
      if (rpt.timestamp && now - rpt.timestamp > MAX_AGE) updates[`/reports_trash/${id}`] = null;
    });
    Object.entries(usersData).forEach(([uid, usr]) => {
      if (usr.lastReportTimestamp && now - usr.lastReportTimestamp > MAX_AGE) updates[`/users/${uid}/lastReportTimestamp`] = null;
    });

    const processed = new Set();
    const entries = Object.entries(liveData);

    for (let i = 0; i < entries.length; i++) {
      const [idA, rA] = entries[i];
      if (processed.has(idA) || !rA.type || !rA.latitude || !rA.longitude || rA.merged) continue;
      const cluster = [[idA, rA]];
      processed.add(idA);
      for (let j = i + 1; j < entries.length; j++) {
        const [idB, rB] = entries[j];
        if (processed.has(idB) || rB.type !== rA.type || rB.merged) continue;
        const dist = haversine(rA.latitude, rA.longitude, rB.latitude, rB.longitude);
        if (dist <= MERGE_RADIUS) {
          cluster.push([idB, rB]);
          processed.add(idB);
        }
      }
      if (cluster.length < 2) continue;

      const mergedDesc = cluster.map(([, rpt]) => rpt.description || "").filter(d => d).join(",");
      const timestamps = cluster.map(([, rpt]) => rpt.timestamp || 0);
      const maxTs = Math.max(...timestamps);
      const avgLat = cluster.reduce((sum, [, rpt]) => sum + rpt.latitude, 0) / cluster.length;
      const avgLon = cluster.reduce((sum, [, rpt]) => sum + rpt.longitude, 0) / cluster.length;
      const newKey = liveRef.push().key;

      updates[`/reports/${newKey}`] = {
        type: rA.type,
        description: mergedDesc,
        latitude: avgLat,
        longitude: avgLon,
        timestamp: maxTs,
        merged: true,
      };

      cluster.forEach(([rid, rpt]) => {
        updates[`/reports_trash/${rid}`] = rpt;
        updates[`/reports/${rid}`] = null;
      });
    }

    await db.ref().update(updates);
  } catch {}
  process.exit(0);
})();
