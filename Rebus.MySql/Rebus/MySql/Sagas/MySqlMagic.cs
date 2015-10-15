using System.Collections.Generic;
using MySql.Data.MySqlClient;

namespace Rebus.MySql.Sagas
{
    public static class MySqlMagic
    {
        /// <summary>
        /// Represents the MySql error number for a primary key violation
        /// </summary>
        public const int PrimaryKeyViolationNumber = 1062;
        public const int ObjectDoesNotExistOrNoPermission = 1146;

        /// <summary>
        /// Gets the names of all tables in the current database
        /// </summary>
        public static List<string> GetTableNames(this MySqlConnection connection, MySqlTransaction transaction = null)
        {
            return GetNamesFrom(connection, transaction, "SHOW TABLES");
        }

        /// <summary>
        /// Gets the names of all databases on the current server
        /// </summary>
        public static List<string> GetDatabaseNames(this MySqlConnection connection, MySqlTransaction transaction = null)
        {
            return GetNamesFrom(connection, transaction, "SHOW SCHEMAS");
        }

        static List<string> GetNamesFrom(MySqlConnection connection, MySqlTransaction transaction, string commandText)
        {
            var names = new List<string>();

            using (var command = connection.CreateCommand())
            {
                if (transaction != null)
                {
                    command.Transaction = transaction;
                }

                command.CommandText = commandText;

                using (var reader = command.ExecuteReader())
                {
                    while (reader.Read())
                    {
                        var name = reader[0].ToString();

                        names.Add(name);
                    }
                }
            }

            return names;
        }
    }
}