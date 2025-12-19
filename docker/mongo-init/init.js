// Initiate replica set (safe if already initiated)
try {
  rs.status();
  print("Replica set already initiated.");
} catch (e) {
  print("Initiating replica set...");
  rs.initiate({
    _id: "rs0",
    members: [
      { _id: 0, host: "mongo1:27017" },
      { _id: 1, host: "mongo2:27017" },
      { _id: 2, host: "mongo3:27017" }
    ]
  });
}

function waitForPrimary() {
  for (let i = 0; i < 60; i++) {
    try {
      const hello = db.adminCommand({ hello: 1 });
      if (hello.isWritablePrimary) return true;
    } catch (e) {}
    sleep(1000);
  }
  return false;
}

if (!waitForPrimary()) {
  throw new Error("Primary not elected in time.");
}

print("Creating database + collections + indexes...");
const d = db.getSiblingDB("edgar");

d.createCollection("raw_filings");
d.createCollection("clean_filings");
d.createCollection("gold_filings_by_form_month");
d.createCollection("gold_top_ciks_by_volume");
d.createCollection("gold_forms_by_year");

// Raw
d.raw_filings.createIndex({ accession_number: 1 }, { unique: true, sparse: true });
d.raw_filings.createIndex({ cik: 1, date_filed: 1 });
d.raw_filings.createIndex({ form_type: 1 });

// Clean
d.clean_filings.createIndex({ accession_number: 1 }, { unique: true, sparse: true });
d.clean_filings.createIndex({ cik: 1, date_filed: 1 });
d.clean_filings.createIndex({ form_type: 1 });

// Gold
d.gold_filings_by_form_month.createIndex({ month: 1, form_type: 1 });
d.gold_forms_by_year.createIndex({ year: 1, form_type: 1 });
d.gold_top_ciks_by_volume.createIndex({ filings_count: -1 });

print("Done.");
