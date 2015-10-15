using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using MySql.Data.MySqlClient;
using Newtonsoft.Json;
using Rebus.Auditing.Sagas;
using Rebus.Logging;
using Rebus.MySql.Persistence;
using Rebus.Sagas;

namespace Rebus.MySql
{
    /// <summary>
    /// Implementation of <see cref="ISagaSnapshotStorage"/> that uses a table in MySql to store saga snapshots
    /// </summary>
    public class MySqlSagaSnapshotStorage : ISagaSnapshotStorage
    {
        readonly IDbConnectionProvider _connectionProvider;
        readonly string _tableName;
        readonly ILog _log;

        static readonly JsonSerializerSettings DataSettings =
            new JsonSerializerSettings { TypeNameHandling = TypeNameHandling.All };

        static readonly JsonSerializerSettings MetadataSettings =
            new JsonSerializerSettings { TypeNameHandling = TypeNameHandling.None };

        /// <summary>
        /// Constructs the snapshot storage
        /// </summary>
        public MySqlSagaSnapshotStorage(IDbConnectionProvider connectionProvider, string tableName, IRebusLoggerFactory rebusLoggerFactory)
        {
            _log = rebusLoggerFactory.GetCurrentClassLogger();
            _connectionProvider = connectionProvider;
            _tableName = tableName;
        }

        /// <summary>
        /// Creates the subscriptions table if necessary
        /// </summary>
        public void EnsureTableIsCreated()
        {
            using (var connection = _connectionProvider.GetConnection().Result)
            {
                var tableNames = connection.GetTableNames();

                if (tableNames.Contains(_tableName, StringComparer.OrdinalIgnoreCase))
                {
                    return;
                }

                _log.Info("Table '{0}' does not exist - it will be created now", _tableName);

                using (var command = connection.CreateCommand())
                {
                    command.CommandText = string.Format(@"
CREATE TABLE `{0}` (
	`id` BINARY(16) NOT NULL,
	`revision` int NOT NULL,
	`data`  LONGTEXT NOT NULL,
	`metadata` LONGTEXT NOT NULL,
    CONSTRAINT `PK_{0}` PRIMARY KEY CLUSTERED 
    (
	    `id` ASC,
        `revision` ASC
    )
)
", _tableName);
                    command.ExecuteNonQuery();
                }

                connection.Complete();
            }
        }

        /// <summary>
        /// Saves a snapshot of the saga data along with the given metadata
        /// </summary>
        public async Task Save(ISagaData sagaData, Dictionary<string, string> sagaAuditMetadata)
        {
            using (var connection = await _connectionProvider.GetConnection())
            {
                using (var command = connection.CreateCommand())
                {
                    command.CommandText = string.Format(@"

INSERT INTO `{0}` (
    `id`,
    `revision`,
    `data`,
    `metadata`
) VALUES (
    @id, 
    @revision, 
    @data,
    @metadata
)

", _tableName);
                    command.Parameters.Add("id", MySqlDbType.Binary).Value = sagaData.Id.ToByteArray();
                    command.Parameters.Add("revision", MySqlDbType.Int32).Value = sagaData.Revision;
                    command.Parameters.Add("data", MySqlDbType.LongText).Value = JsonConvert.SerializeObject(sagaData, DataSettings);
                    command.Parameters.Add("metadata", MySqlDbType.LongText).Value = JsonConvert.SerializeObject(sagaAuditMetadata, MetadataSettings);

                    await command.ExecuteNonQueryAsync();
                }

                await connection.Complete();
            }
        }
    }
}
