using NUnit.Framework;
using Rebus.Logging;
using Rebus.Sagas;
using Rebus.Tests.Contracts.Sagas;
using Rebus.MySql.Persistence;
using Rebus.MySql.Sagas;
using System;
using MySql.Data.MySqlClient;
using System.Linq;
using System.Collections.Generic;

namespace Rebus.MySql.Tests
{
    [TestFixture, Category(MySqlSagaStorageFactory.Category)]
    public class MySqlSagaStorageBasicLoadAndSaveAndFindOperations : BasicLoadAndSaveAndFindOperations<MySqlSagaStorageFactory> { }

    [TestFixture, Category(MySqlSagaStorageFactory.Category)]
    public class MySqlSagaStorageConcurrencyHandling : ConcurrencyHandling<MySqlSagaStorageFactory> { }

    [TestFixture, Category(MySqlSagaStorageFactory.Category)]
    public class MySqlSagaStorageSagaIntegrationTests : SagaIntegrationTests<MySqlSagaStorageFactory> { }

    public class MySqlSagaStorageFactory : ISagaStorageFactory
    {
        public const string Category = "mysql";
        const string IndexTableName = "RebusSagaIndex";
        const string DataTableName = "RebusSagaData";

        public ISagaStorage GetSagaStorage()
        {
            var consoleLoggerFactory = new ConsoleLoggerFactory(true);
            var connectionProvider = new DbConnectionProvider(SqlTestHelper.ConnectionString, consoleLoggerFactory);
            var storage = new MySqlSagaStorage(connectionProvider, DataTableName, IndexTableName, consoleLoggerFactory);

            storage.EnsureTablesAreCreated();

            return storage;
        }

        public void CleanUp()
        {
            SqlTestHelper.DropTable(IndexTableName);
            SqlTestHelper.DropTable(DataTableName);
        }
    }
    public class SqlTestHelper
    {
        static bool _databaseHasBeenInitialized;

        static string _connectionString;

        public static string ConnectionString
        {
            get
            {
                if (_connectionString != null)
                {
                    return _connectionString;
                }

                var databaseName = DatabaseName;

                if (!_databaseHasBeenInitialized)
                {
                    InitializeDatabase(databaseName);
                }

                Console.WriteLine("Using local SQL database {0}", databaseName);

                _connectionString = GetConnectionStringForDatabase(databaseName);

                return _connectionString;
            }
        }

        public static string DatabaseName
        {
            get { return string.Format("rebus2_test_{0}", Rebus.Tests.TestConfig.Suffix).TrimEnd('_'); }
        }

        public static void DropTable(string tableName)
        {
            try
            {
                using (var connection = new MySqlConnection(ConnectionString))
                {
                    connection.Open();

                    if (!connection.GetTableNames().Contains(tableName, StringComparer.InvariantCultureIgnoreCase)) return;

                    Console.WriteLine("Dropping table {0}", tableName);

                    try
                    {
                        using (var command = connection.CreateCommand())
                        {
                            command.CommandText = string.Format("DROP TABLE `{0}`", tableName);
                            command.ExecuteNonQuery();
                        }
                    }
                    catch (MySqlException exception)
                    {
                        if (exception.Number == MySqlMagic.ObjectDoesNotExistOrNoPermission) return;

                        throw;
                    }
                }
            }
            catch (Exception exception)
            {
                DumpWho();

                throw new ApplicationException(string.Format("Could not drop table '{0}'", tableName), exception);
            }
        }

        static void DumpWho()
        {
            try
            {
                Console.WriteLine("Trying to dump all active connections for db {0}...", DatabaseName);
                Console.WriteLine();

                var who = ExecSpWho()
                    .Where(kvp => kvp.ContainsKey("db"))
                    .Where(kvp => kvp["db"].Equals(DatabaseName, StringComparison.InvariantCultureIgnoreCase));

                Console.WriteLine(string.Join(Environment.NewLine,
                    who.Select(d => string.Join(", ", d.Select(kvp => string.Format("{0} = {1}", kvp.Key, kvp.Value))))));

                Console.WriteLine();
            }
            catch (Exception exception)
            {
                Console.WriteLine("Could not execute sp_who: {0}", exception);
            }
        }

        public static IEnumerable<IDictionary<string, string>> ExecSpWho()
        {
            using (var connection = new MySqlConnection(ConnectionString))
            {
                connection.Open();

                using (var command = connection.CreateCommand())
                {
                    command.CommandText = "SHOW FULL PROCESSLIST;";

                    using (var reader = command.ExecuteReader())
                    {
                        var rows = new List<Dictionary<string, string>>();

                        while (reader.Read())
                        {
                            rows.Add(Enumerable.Range(0, reader.FieldCount)
                                .Select(field => new
                            {
                                ColumnName = reader.GetName(field),
                                Value = (reader.GetValue(field) ?? "").ToString().Trim()
                            })
                                .ToDictionary(a => a.ColumnName, a => a.Value));
                        }

                        return rows;
                    }
                }
            }
        }

        static void InitializeDatabase(string databaseName)
        {
            try
            {
                var masterConnectionString = GetConnectionStringForDatabase("test");

                using (var connection = new MySqlConnection(masterConnectionString))
                {
                    connection.Open();

                    if (connection.GetDatabaseNames().Contains(databaseName, StringComparer.InvariantCultureIgnoreCase)) return;

                    Console.WriteLine("Creating database {0}", databaseName);

                    using (var command = connection.CreateCommand())
                    {
                        command.CommandText = string.Format("CREATE DATABASE `{0}`", databaseName);
                        command.ExecuteNonQuery();
                    }
                }

                _databaseHasBeenInitialized = true;

            }
            catch (Exception exception)
            {
                throw new ApplicationException(string.Format("Could not initialize database '{0}'", databaseName), exception);
            }
        }

        static string GetConnectionStringForDatabase(string databaseName)
        {
            return string.Format("Server=localhost; Database={0}; User=test; Password=Pa55word;", databaseName);
        }
    }
}
