import java.sql.PreparedStatement;
import java.sql.SQLException;

/**
 * Created by kitae on 8/15/16.
 */
public class DbAccessionVersion {

    public DbAccessionVersion(String dbname, String accessionName, String VersionName) {
        db = dbname;
        accession = accessionName;
        version = VersionName;
    }

    public void assignStrToPeparedStatement(int ind, PreparedStatement preparedStatement) throws SQLException {
        preparedStatement.setString(3*ind + 1, db);
        preparedStatement.setString(3*ind + 2, accession);
        preparedStatement.setString(3*ind + 3, version);
    }

    public DbAccessionVersion(String dbname, String accessionName) {
        this(dbname, accessionName, "");
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == null) {
            return false;
        }
        if (!DbAccessionVersion.class.isAssignableFrom(obj.getClass())) {
            return false;
        }
        final DbAccessionVersion dbav = (DbAccessionVersion) obj;
        if (db.equals(dbav.db) && accession.equals(dbav.accession)) {
            if (version == null && dbav.version == null) {
                return true;
            }
            if (version != null && dbav.version != null && version.equals(dbav.version)) {
                return true;
            }
        }
        return false;
    }

    @Override
    public String toString(){
        return db + ":" + accession + ":" + version;
    }

    @Override
    public int hashCode() {
        if (version != null) {
            return db.hashCode() + accession.hashCode() + version.hashCode();
        }
        return db.hashCode() + accession.hashCode();
    }

    String db;
    String accession;
    String version;
}
