
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Properties;

/**
 * This class demonstrates how to connect to MySQL and run some basic commands.
 * 
 */
public class DBDemo {

	/** The name of the MySQL account */
	private final String userName = "root";

	/** The password for the MySQL account */
	private final String password = "Kamath7v$";

	/** The name of the computer running MySQL */
	private final String serverName = "localhost";

	/** The port of the MySQL server */
	private final int portNumber = 3306;

	/** The name of the database we are testing with */
	private final String dbName = "books";
	
	/** The name of the table which stores book details */
	private final String books_tableName = "BOOKS";
	
	/** The name of the table which stores author details */
	private final String authors_tableName = "AUTHORS";
	
	/** The name of the table which stores users details */
	private final String users_tableName = "USERS";
	
	/** The name of the table which stores issued book details */
	private final String issuedBooks_tableName = "ISSUED_BOOKS";
	
	/** Dummy table */
	private final String dummy_tableName = "DUMMY";
	
	/**
	 * Get a new database connection
	 * 
	 * @return
	 * @throws SQLException
	 */
	public Connection getConnection() throws SQLException {
		Connection connection = null;
		Properties connectionProps = new Properties();
		connectionProps.put("user", this.userName);
		connectionProps.put("password", this.password);

		connection = DriverManager.getConnection("jdbc:mysql://"
				+ this.serverName + ":" + this.portNumber + "/" + this.dbName,
				connectionProps);

		return connection;
	}

	/**
	 * Execute a SQL command
	 * CREATE/INSERT/UPDATE/DELETE/DROP/etc.
	 * 
	 * @throws SQLException If something goes wrong
	 */
	public boolean executeUpdate(Connection conn, String command) throws SQLException {
	    Statement stmt = null;
	    try {
	        stmt = conn.createStatement();
	        stmt.executeUpdate(command);
	        return true;
	    } finally {

	    	// This will run whether we throw an exception or not
	        if (stmt != null) { stmt.close(); }
	    }
	}
	
	/**
	 * Execute a SQL command
	 * CREATE/INSERT/UPDATE/DELETE/DROP/etc.
	 * 
	 * @throws SQLException If something goes wrong
	 */
	public ResultSet executeSelectQuery(Connection conn, String command) throws SQLException {
	    Statement stmt = null;
	    ResultSet rs = null;
	    stmt = conn.createStatement();
        rs = stmt.executeQuery(command);
        if (stmt.execute(command)) {
            rs = stmt.getResultSet();
        }
        
        return rs;
	}
	
	/**
	 * Connect to MySQL and do some stuff.
	 */
	public void run() {

		// Connect to MySQL
		Connection conn = null;
		try {
			conn = this.getConnection();
			System.out.println("Connected to database");
		} catch (SQLException e) {
			System.out.println("ERROR: Could not connect to the database");
			e.printStackTrace();
			return;
		}

		
		// Create a table "Books"
		try {
			String createString = "CREATE TABLE " + this.books_tableName + " ( " + "BOOK_ID INTEGER NOT NULL, "
					+ "NAME varchar(40) NOT NULL, " + "AUTHOR varchar(40) NOT NULL, " + "GENRE varchar(20) NOT NULL, "
					+ "PAGE_COUNT INTEGER NOT NULL, " + "RATING INTEGER NOT NULL, " + "PRIMARY KEY (BOOK_ID))";
			this.executeUpdate(conn, createString);
			System.out.println("Created the table Books");
		} catch (SQLException e) {
			System.out.println("ERROR: Could not create the table");
			e.printStackTrace();
			return;
		}

		// Create a table "Authors"
		try {
			String createString = "CREATE TABLE " + this.authors_tableName + " ( " + "AUTHOR_ID INTEGER NOT NULL, "
					+ "NAME varchar(40) NOT NULL, " + "EMAIL varchar(60) NOT NULL, " + "PRIMARY KEY (AUTHOR_ID))";
			this.executeUpdate(conn, createString);
			System.out.println("Created the table Authors");
		} catch (SQLException e) {
			System.out.println("ERROR: Could not create the table");
			e.printStackTrace();
			return;
		}

		// Create a table "Users"
		try {
			String createString = "CREATE TABLE " + this.users_tableName + " ( "
					+ "USER_ID INTEGER NOT NULL, " + "NAME varchar(40) NOT NULL, " + "EMAIL varchar(40) NOT NULL, "
					+ "PRIMARY KEY (USER_ID))";
			this.executeUpdate(conn, createString);
			System.out.println("Created the table Users");
		} catch (SQLException e) {
			System.out.println("ERROR: Could not create the table");
			e.printStackTrace();
			return;
		}
		
		// Create a table "Issued Books"
		try {
			String createString = "CREATE TABLE " + this.issuedBooks_tableName + " ( "
					+ "ISSUE_ID INTEGER NOT NULL, " + "USER_ID INTEGER NOT NULL, " 
					+ "BOOK_ID INTEGER NOT NULL, " + "ISSUE_DATE DATE NOT NULL, "
					+ "PRIMARY KEY (ISSUE_ID))";
			this.executeUpdate(conn, createString);
			System.out.println("Created the table Users");
		} catch (SQLException e) {
			System.out.println("ERROR: Could not create the table");
			e.printStackTrace();
			return;
		}		
		
		
		// Create a table "Dummy" for deletion
			
		
		try {
			String createString = "CREATE TABLE " + this.dummy_tableName + " ( "
					+ "DUMMY_ID INTEGER NOT NULL, " + "USER_ID INTEGER NOT NULL, "
					+ "PRIMARY KEY (DUMMY_ID))";
			this.executeUpdate(conn, createString);
			System.out.println("Created the table Dummy");
		} catch (SQLException e) {
			System.out.println("ERROR: Could not create the table");
			e.printStackTrace();
			return;
		}
		
		//Insert into dummy table
		
		try {
			String insertString = "INSERT INTO DUMMY VALUES(1,2);";
			this.executeUpdate(conn, insertString);
			System.out.println("Created the table Users");
		} catch (SQLException e) {
			System.out.println("ERROR: Could not insert");
			e.printStackTrace();
			return;
		}
		
		try {
			System.out.println("\n-------------------------Display dummy table----------------------");
			String selectString = "SELECT * FROM DUMMY;";
			ResultSet rs = this.executeSelectQuery(conn, selectString);
			while (rs.next()) {
		        int dummyID = rs.getInt("DUMMY_ID");
		        int userID = rs.getInt("USER_ID");
		        System.out.println("\n" + dummyID + ", " + userID);
		      }
		} catch (SQLException e) {
			System.out.println("ERROR: Could not select");
			e.printStackTrace();
			return;
		}
		
		try {
			System.out.println("\n-------------------------Display books after delete----------------------");
			String selectString = "SELECT * FROM BOOKS;";
			ResultSet rs = this.executeSelectQuery(conn, selectString);
			while (rs.next()) {
		        int bookID = rs.getInt("BOOK_ID");
		        String name = rs.getString("NAME");
		        String author = rs.getString("AUTHOR");
		        String genre = rs.getString("GENRE");
		        int pageCount = rs.getInt("PAGE_COUNT");
		        int rating = rs.getInt("RATING");
		        System.out.println("\n" + bookID + ", " + name + ", " + author +
		                           ", " + genre + ", " + pageCount + ", " + rating);
		      }
		} catch (SQLException e) {
			System.out.println("ERROR: Could not select");
			e.printStackTrace();
			return;
		}
		
		
		// Insert the values into Books table
		try {
			String insertString = "INSERT INTO BOOKS VALUES("
					+ "1009, " +
					"'" + "Cuckoos Calling" + "'" + ", "
					+ "'" + "JK Rowling" + "'" + ", " + "'" + "Thriller" + "'" +
					", " + "360" + ", " + "87" + ");";
			this.executeUpdate(conn, insertString);
			System.out.println("\nInserted values in the table Books");
		} catch (SQLException e) {
			System.out.println("ERROR: Could not insert");
			e.printStackTrace();
			return;
		}
		
		try {
			System.out.println("\n----------------------Display books after delete---------------------");
			String selectString = "SELECT * FROM BOOKS;";
			ResultSet rs = this.executeSelectQuery(conn, selectString);
			while (rs.next()) {
		        int bookID = rs.getInt("BOOK_ID");
		        String name = rs.getString("NAME");
		        String author = rs.getString("AUTHOR");
		        String genre = rs.getString("GENRE");
		        int pageCount = rs.getInt("PAGE_COUNT");
		        int rating = rs.getInt("RATING");
		        System.out.println("\n" + bookID + ", " + name + ", " + author +
		                           ", " + genre + ", " + pageCount + ", " + rating);
		      }
		} catch (SQLException e) {
			System.out.println("ERROR: Could not select");
			e.printStackTrace();
			return;
		}
		
		
		
		//Select queries
		try {
			System.out.println("\n-------------------Display books with page count between 250 and 300-----------------");
			String selectString = "SELECT * FROM BOOKS WHERE PAGE_COUNT BETWEEN 250 AND 300;";
			ResultSet rs = this.executeSelectQuery(conn, selectString);
			while (rs.next()) {
		        int bookID = rs.getInt("BOOK_ID");
		        String name = rs.getString("NAME");
		        String author = rs.getString("AUTHOR");
		        String genre = rs.getString("GENRE");
		        int pageCount = rs.getInt("PAGE_COUNT");
		        int rating = rs.getInt("RATING");
		        System.out.println("\n" + bookID + ", " + name + ", " + author +
		                           ", " + genre + ", " + pageCount + ", " + rating);
		      }
		} catch (SQLException e) {
			System.out.println("ERROR: Could not select");
			e.printStackTrace();
			return;
		}
		
		// Update the values into Books table
		try {
			String updateString = "UPDATE BOOKS SET PAGE_COUNT=400 WHERE BOOK_ID=1009;";
			this.executeUpdate(conn, updateString);
			System.out.println("\nUpdated values in the table Books");
		} catch (SQLException e) {
			System.out.println("ERROR: Could not update");
			e.printStackTrace();
			return;
		}	
		
		//Select queries
		try {
			System.out.println("-------------------------Display books after update-----------------------");
			String selectString = "SELECT * FROM BOOKS;";
			ResultSet rs = this.executeSelectQuery(conn, selectString);
			while (rs.next()) {
		        int bookID = rs.getInt("BOOK_ID");
		        String name = rs.getString("NAME");
		        String author = rs.getString("AUTHOR");
		        String genre = rs.getString("GENRE");
		        int pageCount = rs.getInt("PAGE_COUNT");
		        int rating = rs.getInt("RATING");
		        System.out.println("\n" + bookID + ", " + name + ", " + author +
		                           ", " + genre + ", " + pageCount + ", " + rating);
		      }
		} catch (SQLException e) {
			System.out.println("ERROR: Could not select");
			e.printStackTrace();
			return;
		}
		
		// Delete the values into Books table
		try {
			String updateString = "DELETE FROM BOOKS WHERE BOOK_ID=1009;";
			this.executeUpdate(conn, updateString);
			System.out.println("\nDeleted values in the table Books");
		} catch (SQLException e) {
			System.out.println("ERROR: Could not delete");
			e.printStackTrace();
			return;
		}
		
		try {
			System.out.println("---------------------------Display books after delete--------------------------");
			String selectString = "SELECT * FROM BOOKS;";
			ResultSet rs = this.executeSelectQuery(conn, selectString);
			while (rs.next()) {
		        int bookID = rs.getInt("BOOK_ID");
		        String name = rs.getString("NAME");
		        String author = rs.getString("AUTHOR");
		        String genre = rs.getString("GENRE");
		        int pageCount = rs.getInt("PAGE_COUNT");
		        int rating = rs.getInt("RATING");
		        System.out.println("\n" + bookID + ", " + name + ", " + author +
		                           ", " + genre + ", " + pageCount + ", " + rating);
		      }
		} catch (SQLException e) {
			System.out.println("ERROR: Could not select");
			e.printStackTrace();
			return;
		}
		
		
		
		
		// Drop the dummy table
		try {
			String dropString = "DROP TABLE " + this.dummy_tableName;
			this.executeUpdate(conn, dropString);
			System.out.println("Dropped the table");
		} catch (SQLException e) {
			System.out.println("ERROR: Could not drop the table");
			e.printStackTrace();
			return;
		}

		
	}
	
	/**
	 * Connect to the DB and do some stuff
	 */
	public static void main(String[] args) {
		DBDemo app = new DBDemo();
		app.run();
	}
}
