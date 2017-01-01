package org.warnup.heron.tweets.tools;


import java.sql.*;
import java.util.Properties;


public class DBWriter {

    private static String dbUrl, dbUser, dbPassword;
    Connection conn;


    public DBWriter() throws Exception{

        try {

            Properties properties = new Properties();
            properties.load(DBWriter.class.getResourceAsStream("/config.properties"));
            dbUrl = properties.getProperty("db.url");
            dbUser = properties.getProperty("db.user");
            dbPassword = properties.getProperty("db.password");
        }catch (Exception e) {
            System.out.println(e.getMessage());
        }

        conn = this.connect();

    }


    private Connection connect() {

        Properties props = new Properties();
        props.setProperty("user", dbUser);
        props.setProperty("password", dbPassword);
        Connection conn = null;


        try {
            Class.forName("org.postgresql.Driver");
            conn = DriverManager.getConnection(dbUrl, props);
        }
        catch (SQLException e) {
            System.out.println(e.getMessage());
        } catch ( Exception e ) {
            System.err.println( e.getClass().getName() + ": " + e.getMessage() );
            System.exit(0);
        }
        return conn;
    }


    public void insert(long tweet_id, int count) {

        String sql = "INSERT INTO tweets (time, tweet_id, count, type) VALUES (?,?,?,?)";
        long epoch_time = System.currentTimeMillis()/1000;
        int type = 1; //so far default for news

        try{
            PreparedStatement pstmt = conn.prepareStatement(sql);
            pstmt.setLong(1, epoch_time);
            pstmt.setLong(2, tweet_id);
            pstmt.setInt(3, count);
            pstmt.setInt(4, type);
            pstmt.executeUpdate();

        } catch (SQLException e) {
            System.out.println(e.getMessage());
        }
    }
}
