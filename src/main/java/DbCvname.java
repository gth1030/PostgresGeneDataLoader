/**
 * Created by kitae on 8/15/16.
 */
public class DbCvname {
    public DbCvname(String dbname, String cvName) {
        DbName = dbname;
        CvName = cvName;
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == null) {
            return false;
        }
        if (!DbCvname.class.isAssignableFrom(obj.getClass())) {
            return false;
        }
        final DbCvname cvObj = (DbCvname) obj;
        if (DbName.equals(cvObj.DbName) && CvName.equals(cvObj.CvName)) {
            return true;
        }
        return false;
    }

    @Override
    public String toString(){
        return DbName + ":" + CvName;
    }

    @Override
    public int hashCode() {
        return CvName.hashCode() + DbName.hashCode();
    }

    String DbName;
    String CvName;
}
