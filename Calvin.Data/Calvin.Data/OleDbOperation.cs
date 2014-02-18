using System;
using System.Collections.Generic;
using System.Text;
using System.Data;
using System.Data.OleDb;
namespace Calvin.Data
{
   public class OleDbOperation
    {

        private readonly string _connectionString;

        public OleDbOperation(string connectionString)
        {

            _connectionString = connectionString;
        }

        public OleDbConnection GetConnection()
        {
            OleDbConnection conn = new OleDbConnection();
            conn.ConnectionString = _connectionString;
            conn.Open();
            return conn;
        }

        public void RecycleConnection(OleDbConnection connection)
        {
            if (connection == null)
            {
                return;
            }
            connection.Close();
            connection.Dispose();
        }
        #region ExecuteDataSet
        public DataSet ExecuteDataSet(string sql)
        {
            DataSet set;
            OleDbConnection connection = this.GetConnection();
            try
            {
                set = ExecuteDataSet(sql, CommandType.Text, connection, null, null);
            }
            finally
            {
                this.RecycleConnection(connection);
            }
            return set;
        }

        public static DataSet ExecuteDataSet(string sql, OleDbConnection connection)
        {
            return ExecuteDataSet(sql, CommandType.Text, connection, null, null);
        }

        public DataSet ExecuteDataSet(string sql, CommandType commandType)
        {
            DataSet set;
            OleDbConnection connection = this.GetConnection();
            try
            {
                set = ExecuteDataSet(sql, commandType, connection, null, null);
            }
            finally
            {
                this.RecycleConnection(connection);
            }
            return set;
        }

        public DataSet ExecuteDataSet(string sql, IsolationLevel iso)
        {
            DataSet set2;
            OleDbConnection connection = this.GetConnection();
            OleDbTransaction transaction = null;
            try
            {
                transaction = connection.BeginTransaction(iso);
                DataSet set = ExecuteDataSet(sql, CommandType.Text, connection, transaction, null);
                transaction.Commit();
                set2 = set;
            }
            catch
            {
                if (transaction != null)
                {
                    transaction.Rollback();
                }
                throw;
            }
            finally
            {
                this.RecycleConnection(connection);
            }
            return set2;
        }

        public DataSet ExecuteDataSet(string sql, OleDbParameter[] parameters)
        {
            DataSet set;
            OleDbConnection connection = this.GetConnection();
            try
            {
                set = ExecuteDataSet(sql, CommandType.Text, connection, null, parameters);
            }
            finally
            {
                this.RecycleConnection(connection);
            }
            return set;
        }

        public static DataSet ExecuteDataSet(string sql, OleDbConnection connection, OleDbTransaction transaction)
        {
            return ExecuteDataSet(sql, CommandType.Text, connection, transaction, null);
        }

        public static DataSet ExecuteDataSet(string sql, OleDbConnection connection, OleDbParameter[] parameters)
        {
            return ExecuteDataSet(sql, CommandType.Text, connection, null, parameters);
        }

        public static DataSet ExecuteDataSet(string sql, CommandType commandType, OleDbConnection connection)
        {
            return ExecuteDataSet(sql, commandType, connection, null, null);
        }

        public DataSet ExecuteDataSet(string sql, CommandType commandType, IsolationLevel iso)
        {
            DataSet set2;
            OleDbConnection connection = this.GetConnection();
            OleDbTransaction    transaction = null;
            try
            {
                transaction = connection.BeginTransaction(iso);
                DataSet set = ExecuteDataSet(sql, commandType, connection, transaction, null);
                transaction.Commit();
                set2 = set;
            }
            catch
            {
                if (transaction != null)
                {
                    transaction.Rollback();
                }
                throw;
            }
            finally
            {
                this.RecycleConnection(connection);
            }
            return set2;
        }

        public DataSet ExecuteDataSet(string sql, CommandType commandType, OleDbParameter[] parameters)
        {
            DataSet set;
            OleDbConnection connection = this.GetConnection();
            try
            {
                set = ExecuteDataSet(sql, commandType, connection, null, parameters);
            }
            finally
            {
                this.RecycleConnection(connection);
            }
            return set;
        }

        public DataSet ExecuteDataSet(string sql, OleDbParameter[] parameters, IsolationLevel iso)
        {
            DataSet set2;
            OleDbConnection connection = this.GetConnection();
            OleDbTransaction transaction = null;
            try
            {
                transaction = connection.BeginTransaction(iso);
                DataSet set = ExecuteDataSet(sql, CommandType.Text, connection, transaction, parameters);
                transaction.Commit();
                set2 = set;
            }
            catch
            {
                if (transaction != null)
                {
                    transaction.Rollback();
                }
                throw;
            }
            finally
            {
                this.RecycleConnection(connection);
            }
            return set2;
        }

        public static DataSet ExecuteDataSet(string sql, OleDbConnection connection, OleDbTransaction transaction, OleDbParameter[] parameters)
        {
            return ExecuteDataSet(sql, CommandType.Text, connection, transaction, parameters);
        }

        public static DataSet ExecuteDataSet(string sql, CommandType commandType, OleDbConnection connection, OleDbTransaction transaction)
        {
            return ExecuteDataSet(sql, commandType, connection, transaction, null);
        }

        public static DataSet ExecuteDataSet(string sql, CommandType commandType, OleDbConnection connection, OleDbParameter[] parameters)
        {
            return ExecuteDataSet(sql, commandType, connection, null, parameters);
        }

        public DataSet ExecuteDataSet(string sql, CommandType commandType, OleDbParameter[] parameters, IsolationLevel iso)
        {
            DataSet set2;
            OleDbConnection connection = this.GetConnection();
            OleDbTransaction transaction = null;
            try
            {
                transaction = connection.BeginTransaction(iso);
                DataSet set = ExecuteDataSet(sql, commandType, connection, transaction, parameters);
                transaction.Commit();
                set2 = set;
            }
            catch
            {
                if (transaction != null)
                {
                    transaction.Rollback();
                }
                throw;
            }
            finally
            {
                this.RecycleConnection(connection);
            }
            return set2;
        }

        public static DataSet ExecuteDataSet(string sql, CommandType commandType, OleDbConnection connection, OleDbTransaction transaction, OleDbParameter[] parameters)
        {
            OleDbCommand selectCommand = new OleDbCommand
            {
                Connection = connection,
                CommandText = sql,
                CommandType = commandType,
                Transaction = transaction
            };
            if ((parameters != null) && (parameters.Length > 0))
            {
                selectCommand.Parameters.AddRange(parameters);
            }
            OleDbDataAdapter adapter = new OleDbDataAdapter(selectCommand);
            DataSet dataSet = new DataSet();
            adapter.Fill(dataSet);
            return dataSet;
        }
        #endregion
        #region ExecuteNonQuery
        public int ExecuteNonQuery(string sql)
        {
            int num;
            OleDbConnection connection = this.GetConnection();
            try
            {
                num = ExecuteNonQuery(sql, CommandType.Text, connection, null, null);
            }
            finally
            {
                this.RecycleConnection(connection);
            }
            return num;
        }

        public static int ExecuteNonQuery(string sql, OleDbConnection connection)
        {
            return ExecuteNonQuery(sql, CommandType.Text, connection, null, null);
        }

        public int ExecuteNonQuery(string sql, CommandType commandType)
        {
            int num;
            OleDbConnection connection = this.GetConnection();
            try
            {
                num = ExecuteNonQuery(sql, commandType, connection, null, null);
            }
            finally
            {
                this.RecycleConnection(connection);
            }
            return num;
        }

        public int ExecuteNonQuery(string sql, IsolationLevel iso)
        {
            int num2;
            OleDbConnection connection = this.GetConnection();
            OleDbTransaction transaction = null;
            try
            {
                transaction = connection.BeginTransaction(iso);
                int num = ExecuteNonQuery(sql, CommandType.Text, connection, transaction, null);
                transaction.Commit();
                num2 = num;
            }
            catch
            {
                if (transaction != null)
                {
                    transaction.Rollback();
                }
                throw;
            }
            finally
            {
                this.RecycleConnection(connection);
            }
            return num2;
        }

        public int ExecuteNonQuery(string sql, OleDbParameter[] parameters)
        {
            int num;
            OleDbConnection connection = this.GetConnection();
            try
            {
                num = ExecuteNonQuery(sql, CommandType.Text, connection, null, parameters);
            }
            finally
            {
                this.RecycleConnection(connection);
            }
            return num;
        }

        public static int ExecuteNonQuery(string sql, OleDbConnection connection, OleDbTransaction transaction)
        {
            return ExecuteNonQuery(sql, CommandType.Text, connection, transaction, null);
        }

        public static int ExecuteNonQuery(string sql, OleDbConnection connection, OleDbParameter[] parameters)
        {
            return ExecuteNonQuery(sql, CommandType.Text, connection, null, parameters);
        }

        public static int ExecuteNonQuery(string sql, CommandType commandType, OleDbConnection connection)
        {
            return ExecuteNonQuery(sql, commandType, connection, null, null);
        }

        public int ExecuteNonQuery(string sql, CommandType commandType, IsolationLevel iso)
        {
            int num2;
            OleDbConnection connection = this.GetConnection();
            OleDbTransaction transaction = null;
            try
            {
                transaction = connection.BeginTransaction(iso);
                int num = ExecuteNonQuery(sql, commandType, connection, transaction, null);
                transaction.Commit();
                num2 = num;
            }
            catch
            {
                if (transaction != null)
                {
                    transaction.Rollback();
                }
                throw;
            }
            finally
            {
                this.RecycleConnection(connection);
            }
            return num2;
        }

        public int ExecuteNonQuery(string sql, CommandType commandType, OleDbParameter[] parameters)
        {
            int num;
            OleDbConnection connection = this.GetConnection();
            try
            {
                num = ExecuteNonQuery(sql, commandType, connection, null, parameters);
            }
            finally
            {
                this.RecycleConnection(connection);
            }
            return num;
        }

        public int ExecuteNonQuery(string sql, OleDbParameter[] parameters, IsolationLevel iso)
        {
            int num2;
            OleDbConnection connection = this.GetConnection();
            OleDbTransaction transaction = null;
            try
            {
                transaction = connection.BeginTransaction(iso);
                int num = ExecuteNonQuery(sql, CommandType.Text, connection, transaction, parameters);
                transaction.Commit();
                num2 = num;
            }
            catch
            {
                if (transaction != null)
                {
                    transaction.Rollback();
                }
                throw;
            }
            finally
            {
                this.RecycleConnection(connection);
            }
            return num2;
        }

        public static int ExecuteNonQuery(string sql, OleDbConnection connection, OleDbTransaction transaction, OleDbParameter[] parameters)
        {
            return ExecuteNonQuery(sql, CommandType.Text, connection, transaction, parameters);
        }

        public static int ExecuteNonQuery(string sql, CommandType commandType, OleDbConnection connection, OleDbTransaction transaction)
        {
            return ExecuteNonQuery(sql, commandType, connection, transaction, null);
        }

        public static int ExecuteNonQuery(string sql, CommandType commandType, OleDbConnection connection, OleDbParameter[] parameters)
        {
            return ExecuteNonQuery(sql, commandType, connection, null, parameters);
        }

        public int ExecuteNonQuery(string sql, CommandType commandType, OleDbParameter[] parameters, IsolationLevel iso)
        {
            int num2;
            OleDbConnection connection = this.GetConnection();
            OleDbTransaction transaction = null;
            try
            {
                transaction = connection.BeginTransaction(iso);
                int num = ExecuteNonQuery(sql, commandType, connection, transaction, parameters);
                transaction.Commit();
                num2 = num;
            }
            catch
            {
                if (transaction != null)
                {
                    transaction.Rollback();
                }
                throw;
            }
            finally
            {
                this.RecycleConnection(connection);
            }
            return num2;
        }

        public static int ExecuteNonQuery(string sql, CommandType commandType, OleDbConnection connection, OleDbTransaction transaction, OleDbParameter[] parameters)
        {
            OleDbCommand command = new OleDbCommand
            {
                Connection = connection,
                CommandText = sql,
                CommandType = commandType,
                Transaction = transaction
            };
            if ((parameters != null) && (parameters.Length > 0))
            {
                command.Parameters.AddRange(parameters);
            }
            return command.ExecuteNonQuery();
        }
        #endregion
        #region ExecuteReader
        public static OleDbDataReader ExecuteReader(string sql, OleDbConnection connection)
        {
            return ExecuteReader(sql, CommandType.Text, connection, null, null);
        }

        public static OleDbDataReader ExecuteReader(string sql, OleDbConnection connection, OleDbTransaction transaction)
        {
            return ExecuteReader(sql, CommandType.Text, connection, transaction, null);
        }

        public static OleDbDataReader ExecuteReader(string sql, OleDbConnection connection, OleDbParameter[] parameters)
        {
            return ExecuteReader(sql, CommandType.Text, connection, null, parameters);
        }

        public static OleDbDataReader ExecuteReader(string sql, CommandType commandType, OleDbConnection connection)
        {
            return ExecuteReader(sql, commandType, connection, null, null);
        }

        public static OleDbDataReader ExecuteReader(string sql, OleDbConnection connection, OleDbTransaction transaction, OleDbParameter[] parameters)
        {
            return ExecuteReader(sql, CommandType.Text, connection, transaction, parameters);
        }

        public static OleDbDataReader ExecuteReader(string sql, CommandType commandType, OleDbConnection connection, OleDbTransaction transaction)
        {
            return ExecuteReader(sql, commandType, connection, transaction, null);
        }

        public static OleDbDataReader ExecuteReader(string sql, CommandType commandType, OleDbConnection connection, OleDbParameter[] parameters)
        {
            return ExecuteReader(sql, commandType, connection, null, parameters);
        }

        public static OleDbDataReader ExecuteReader(string sql, CommandType commandType, OleDbConnection connection, OleDbTransaction transaction, OleDbParameter[] parameters)
        {
            OleDbCommand command = new OleDbCommand
            {
                Connection = connection,
                CommandText = sql,
                CommandType = commandType,
                Transaction = transaction
            };
            if ((parameters != null) && (parameters.Length > 0))
            {
                command.Parameters.AddRange(parameters);
            }
            return command.ExecuteReader();
        }
        #endregion
        #region ExecuteScalar
        public object ExecuteScalar(string sql)
        {
            object obj2;
            OleDbConnection connection = this.GetConnection();
            try
            {
                obj2 = ExecuteScalar(sql, CommandType.Text, connection, null, null);
            }
            finally
            {
                this.RecycleConnection(connection);
            }
            return obj2;
        }

        public static object ExecuteScalar(string sql, OleDbConnection connection)
        {
            return ExecuteScalar(sql, CommandType.Text, connection, null, null);
        }

        public object ExecuteScalar(string sql, CommandType commandType)
        {
            object obj2;
            OleDbConnection connection = this.GetConnection();
            try
            {
                obj2 = ExecuteScalar(sql, commandType, connection, null, null);
            }
            finally
            {
                this.RecycleConnection(connection);
            }
            return obj2;
        }

        public object ExecuteScalar(string sql, IsolationLevel iso)
        {
            object obj3;
            OleDbConnection connection = this.GetConnection();
            OleDbTransaction transaction = null;
            try
            {
                transaction = connection.BeginTransaction(iso);
                object obj2 = ExecuteScalar(sql, CommandType.Text, connection, transaction, null);
                transaction.Commit();
                obj3 = obj2;
            }
            catch
            {
                if (transaction != null)
                {
                    transaction.Rollback();
                }
                throw;
            }
            finally
            {
                this.RecycleConnection(connection);
            }
            return obj3;
        }

        public object ExecuteScalar(string sql, OleDbParameter[] parameters)
        {
            object obj2;
            OleDbConnection connection = this.GetConnection();
            try
            {
                obj2 = ExecuteScalar(sql, CommandType.Text, connection, null, parameters);
            }
            finally
            {
                this.RecycleConnection(connection);
            }
            return obj2;
        }

        public static object ExecuteScalar(string sql, OleDbConnection connection, OleDbTransaction transaction)
        {
            return ExecuteScalar(sql, CommandType.Text, connection, transaction, null);
        }

        public static object ExecuteScalar(string sql, OleDbConnection connection, OleDbParameter[] parameters)
        {
            return ExecuteScalar(sql, CommandType.Text, connection, null, parameters);
        }

        public static object ExecuteScalar(string sql, CommandType commandType, OleDbConnection connection)
        {
            return ExecuteScalar(sql, commandType, connection, null, null);
        }

        public object ExecuteScalar(string sql, CommandType commandType, IsolationLevel iso)
        {
            object obj3;
            OleDbConnection connection = this.GetConnection();
            OleDbTransaction transaction = null;
            try
            {
                transaction = connection.BeginTransaction(iso);
                object obj2 = ExecuteScalar(sql, CommandType.Text, connection, transaction, null);
                transaction.Commit();
                obj3 = obj2;
            }
            catch
            {
                if (transaction != null)
                {
                    transaction.Rollback();
                }
                throw;
            }
            finally
            {
                this.RecycleConnection(connection);
            }
            return obj3;
        }

        public object ExecuteScalar(string sql, CommandType commandType, OleDbParameter[] parameters)
        {
            object obj2;
            OleDbConnection connection = this.GetConnection();
            try
            {
                obj2 = ExecuteScalar(sql, commandType, connection, null, parameters);
            }
            finally
            {
                this.RecycleConnection(connection);
            }
            return obj2;
        }

        public object ExecuteScalar(string sql, OleDbParameter[] parameters, IsolationLevel iso)
        {
            object obj3;
            OleDbConnection connection = this.GetConnection();
            OleDbTransaction transaction = null;
            try
            {
                transaction = connection.BeginTransaction(iso);
                object obj2 = ExecuteScalar(sql, CommandType.Text, connection, transaction, parameters);
                transaction.Commit();
                obj3 = obj2;
            }
            catch
            {
                if (transaction != null)
                {
                    transaction.Rollback();
                }
                throw;
            }
            finally
            {
                this.RecycleConnection(connection);
            }
            return obj3;
        }

        public static object ExecuteScalar(string sql, OleDbConnection connection, OleDbTransaction transaction, OleDbParameter[] parameters)
        {
            return ExecuteScalar(sql, CommandType.Text, connection, transaction, parameters);
        }

        public static object ExecuteScalar(string sql, CommandType commandType, OleDbConnection connection, OleDbTransaction transaction)
        {
            return ExecuteScalar(sql, commandType, connection, transaction, null);
        }

        public static object ExecuteScalar(string sql, CommandType commandType, OleDbConnection connection, OleDbParameter[] parameters)
        {
            return ExecuteScalar(sql, commandType, connection, null, parameters);
        }

        public object ExecuteScalar(string sql, CommandType commandType, OleDbParameter[] parameters, IsolationLevel iso)
        {
            object obj3;
            OleDbConnection connection = this.GetConnection();
            OleDbTransaction transaction = null;
            try
            {
                transaction = connection.BeginTransaction(iso);
                object obj2 = ExecuteScalar(sql, CommandType.Text, connection, transaction, parameters);
                transaction.Commit();
                obj3 = obj2;
            }
            catch
            {
                if (transaction != null)
                {
                    transaction.Rollback();
                }
                throw;
            }
            finally
            {
                this.RecycleConnection(connection);
            }
            return obj3;
        }

        public static object ExecuteScalar(string sql, CommandType commandType, OleDbConnection connection, OleDbTransaction transaction, OleDbParameter[] parameters)
        {
            OleDbCommand command = new OleDbCommand
            {
                Connection = connection,
                CommandText = sql,
                CommandType = commandType,
                Transaction = transaction
            };
            if ((parameters != null) && (parameters.Length > 0))
            {
                command.Parameters.AddRange(parameters);
            }
            return command.ExecuteScalar();
        }
        #endregion
    }
}
