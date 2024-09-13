using Dapper;
using DuckDB.NET.Data;

DuckDBConnection versionConn = new DuckDBConnection();
var version = versionConn.ServerVersion;

File.Delete($"D:\\demo_{version}.duck");

using DuckDBConnection duck = new DuckDBConnection($"Data Source=D:\\demo_{version}.duck");
duck.Open();
duck.Execute($"CREATE TABLE SalesActuals AS SELECT * FROM read_parquet('sales_actuals.parquet');");
duck.Execute($"CREATE TABLE SalesUsers AS SELECT * FROM read_parquet('sales_users.parquet');");
duck.Execute($"CREATE TABLE Sales AS SELECT * FROM SalesActuals sa FULL JOIN SalesUsers users USING(CalendarID,ItemID,CountryID,ChannelDemandID,PriceType)");
/* Create a unique index on Sales table */
duck.Execute("CREATE UNIQUE INDEX sales_idx ON Sales (CalendarID,ItemID,CountryID,ChannelDemandID,PriceType);");
/* Ensure that the neccessary rows exist in the Sales table if they do not already */
duck.Execute($@"INSERT OR IGNORE INTO Sales (CalendarID,ItemID,CountryID,ChannelDemandID,PriceType)
                SELECT *
                FROM
	                range(202505::INTEGER,202625::INTEGER) Calendar(CalendarID) 
                    CROSS JOIN (VALUES (103),(550),(9693)) Items(ItemID)
                    CROSS JOIN (VALUES (1)) Countries(CountryID)
                    CROSS JOIN (VALUES (3),(5),(7),(9)) Chanels(ChannelDemandID)
                    CROSS JOIN (SELECT DISTINCT PriceType FROM Sales) AS PriceTypes");
duck.Query(@"SELECT CalendarID,ItemID,CountryID,ChannelDemandID,PriceType 
            FROM Sales 
            WHERE 
                CalendarID = 202526 AND ItemID = 550 
            GROUP BY CalendarID,ItemID,CountryID,ChannelDemandID,PriceType
            HAVING COUNT(*) >1 ORDER BY ChannelDemandID, PriceType"
            ).ToList().ForEach(Console.WriteLine);
Console.ReadLine();
