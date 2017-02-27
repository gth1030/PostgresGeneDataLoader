/**
 * Created by kitae on 2/24/17.
 */
public class DbxrefTuple {

    public DbxrefTuple (String dbID, String accessionN) {
        db_id = dbID;
        accession = accessionN;
    }

    @Override
    public int hashCode() {
        return db_id.hashCode() + accession.hashCode();
    }

    @Override
    public boolean equals(Object tuple) {
        if (tuple == null)
            return false;
        if (!DbxrefTuple.class.isAssignableFrom(tuple.getClass()))
            return false;
        DbxrefTuple obj2 = (DbxrefTuple) tuple;
        if (db_id.equals(obj2.db_id) && accession.equals(obj2.accession)) {
            return true;
        }
        return false;
    }

    String dbxref_id;
    String db_id;
    String accession;
}
