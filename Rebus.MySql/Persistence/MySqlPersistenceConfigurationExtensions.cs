using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Rebus.Auditing.Sagas;
using Rebus.Config;
using Rebus.Logging;
using Rebus.MySql.Sagas;
using Rebus.Sagas;

namespace Rebus.MySql.Persistence
{
    /// <summary>
    /// Configuration extensions for configuring SQL persistence for sagas, subscriptions, and timeouts.
    /// </summary>
    public static class MySqlPersistenceConfigurationExtensions
    {
       /// <summary>
        /// Configures Rebus to use MySql to store sagas, using the tables specified to store data and indexed properties respectively.
        /// </summary>
        public static void StoreInMySql(this StandardConfigurer<ISagaStorage> configurer,
            string connectionStringOrConnectionStringName, string dataTableName, string indexTableName,
            bool automaticallyCreateTables = true)
        {
            configurer.Register(c =>
            {
                var rebusLoggerFactory = c.Get<IRebusLoggerFactory>();
                var connectionProvider = new DbConnectionProvider(connectionStringOrConnectionStringName, rebusLoggerFactory);
                var sagaStorage = new MySqlSagaStorage(connectionProvider, dataTableName, indexTableName, rebusLoggerFactory);

                if (automaticallyCreateTables)
                {
                    sagaStorage.EnsureTablesAreCreated();
                }

                return sagaStorage;
            });
        }
        /*
        /// <summary>
        /// Configures Rebus to use MySql to store subscriptions. Use <paramref name="isCentralized"/> = true to indicate whether it's OK to short-circuit
        /// subscribing and unsubscribing by manipulating the subscription directly from the subscriber or just let it default to false to preserve the
        /// default behavior.
        /// </summary>
        public static void StoreInMySql(this StandardConfigurer<ISubscriptionStorage> configurer,
            string connectionStringOrConnectionStringName, string tableName, bool isCentralized = false, bool automaticallyCreateTables = true)
        {
            configurer.Register(c =>
            {
                var rebusLoggerFactory = c.Get<IRebusLoggerFactory>();
                var connectionProvider = new DbConnectionProvider(connectionStringOrConnectionStringName, rebusLoggerFactory);
                var subscriptionStorage = new MySqlSubscriptionStorage(connectionProvider, tableName, isCentralized, rebusLoggerFactory);

                if (automaticallyCreateTables)
                {
                    subscriptionStorage.EnsureTableIsCreated();
                }

                return subscriptionStorage;
            });
        }

        /// <summary>
        /// Configures Rebus to use MySql to store timeouts.
        /// </summary>
        public static void StoreInMySql(this StandardConfigurer<ITimeoutManager> configurer, string connectionStringOrConnectionStringName, string tableName, bool automaticallyCreateTables = true)
        {
            configurer.Register(c =>
            {
                var rebusLoggerFactory = c.Get<IRebusLoggerFactory>();
                var connectionProvider = new DbConnectionProvider(connectionStringOrConnectionStringName, rebusLoggerFactory);
                var subscriptionStorage = new MySqlTimeoutManager(connectionProvider, tableName, rebusLoggerFactory);

                if (automaticallyCreateTables)
                {
                    subscriptionStorage.EnsureTableIsCreated();
                }

                return subscriptionStorage;
            });
        }
         /// <summary>
        /// Configures Rebus to store saga snapshots in MySql
        /// </summary>
        public static void StoreInMySql(this StandardConfigurer<ISagaSnapshotStorage> configurer,
            string connectionStringOrConnectionStringName, string tableName, bool automaticallyCreateTables = true)
        {
            configurer.Register(c =>
            {
                var rebusLoggerFactory = c.Get<IRebusLoggerFactory>();
                var connectionProvider = new DbConnectionProvider(connectionStringOrConnectionStringName, rebusLoggerFactory);
                var snapshotStorage = new MySqlSagaSnapshotStorage(connectionProvider, tableName, rebusLoggerFactory);

                if (automaticallyCreateTables)
                {
                    snapshotStorage.EnsureTableIsCreated();
                }

                return snapshotStorage;
            });
        }*/
    }
}
