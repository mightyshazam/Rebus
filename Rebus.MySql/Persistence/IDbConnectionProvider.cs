using System.Data.SqlClient;
using System.Threading.Tasks;
using MySql.Data.MySqlClient;
namespace Rebus.MySql.Persistence
{
    /// <summary>
    /// MySQL database connection provider that allows for easily changing how the current <see cref="MySqlConnection"/> is obtained,
    /// possibly also changing how transactions are handled
    /// </summary>
    public interface IDbConnectionProvider
    {
        /// <summary>
        /// Gets a wrapper with the current <see cref="MySqlConnection"/> inside
        /// </summary>
        Task<IDbConnection> GetConnection();
    }
}