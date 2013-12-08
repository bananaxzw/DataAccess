/********************************************************************************************
 * 设计人员:     bananaxzw@qq.com
 * 功能描述:    oracle操作帮助                     
 *              
 * 注意事项:    
 *      
 * 版权所有:    
 * 
 * 修改记录:    修改时间        人员        修改备注
 *              ----------      ------      -------------------------------------------------
 *                           
 * ********************************************************************************************/

using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Data;
using Oracle.DataAccess.Client;

namespace Calvin.Data
{
    public class OracleOperation : Calvin.Data.IDBOperation
    {
        private readonly string _connectionString;

        public OracleOperation(string connectionString)
        {

            _connectionString = connectionString;
        }

        public OracleConnection GetConnection()
        {
            OracleConnection conn = new OracleConnection();
            conn.ConnectionString = _connectionString;
            conn.Open();
            return conn;
        }

        public void RecycleConnection(OracleConnection connection)
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
            OracleConnection connection = this.GetConnection();
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

        public static DataSet ExecuteDataSet(string sql, OracleConnection connection)
        {
            return ExecuteDataSet(sql, CommandType.Text, connection, null, null);
        }

        public DataSet ExecuteDataSet(string sql, CommandType commandType)
        {
            DataSet set;
            OracleConnection connection = this.GetConnection();
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
            OracleConnection connection = this.GetConnection();
            OracleTransaction transaction = null;
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

        public DataSet ExecuteDataSet(string sql, OracleParameter[] parameters)
        {
            DataSet set;
            OracleConnection connection = this.GetConnection();
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

        public static DataSet ExecuteDataSet(string sql, OracleConnection connection, OracleTransaction transaction)
        {
            return ExecuteDataSet(sql, CommandType.Text, connection, transaction, null);
        }

        public static DataSet ExecuteDataSet(string sql, OracleConnection connection, OracleParameter[] parameters)
        {
            return ExecuteDataSet(sql, CommandType.Text, connection, null, parameters);
        }

        public static DataSet ExecuteDataSet(string sql, CommandType commandType, OracleConnection connection)
        {
            return ExecuteDataSet(sql, commandType, connection, null, null);
        }

        public DataSet ExecuteDataSet(string sql, CommandType commandType, IsolationLevel iso)
        {
            DataSet set2;
            OracleConnection connection = this.GetConnection();
            OracleTransaction transaction = null;
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

        public DataSet ExecuteDataSet(string sql, CommandType commandType, OracleParameter[] parameters)
        {
            DataSet set;
            OracleConnection connection = this.GetConnection();
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

        public DataSet ExecuteDataSet(string sql, OracleParameter[] parameters, IsolationLevel iso)
        {
            DataSet set2;
            OracleConnection connection = this.GetConnection();
            OracleTransaction transaction = null;
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

        public static DataSet ExecuteDataSet(string sql, OracleConnection connection, OracleTransaction transaction, OracleParameter[] parameters)
        {
            return ExecuteDataSet(sql, CommandType.Text, connection, transaction, parameters);
        }

        public static DataSet ExecuteDataSet(string sql, CommandType commandType, OracleConnection connection, OracleTransaction transaction)
        {
            return ExecuteDataSet(sql, commandType, connection, transaction, null);
        }

        public static DataSet ExecuteDataSet(string sql, CommandType commandType, OracleConnection connection, OracleParameter[] parameters)
        {
            return ExecuteDataSet(sql, commandType, connection, null, parameters);
        }

        public DataSet ExecuteDataSet(string sql, CommandType commandType, OracleParameter[] parameters, IsolationLevel iso)
        {
            DataSet set2;
            OracleConnection connection = this.GetConnection();
            OracleTransaction transaction = null;
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

        public static DataSet ExecuteDataSet(string sql, CommandType commandType, OracleConnection connection, OracleTransaction transaction, OracleParameter[] parameters)
        {
            OracleCommand selectCommand = new OracleCommand
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
            OracleDataAdapter adapter = new OracleDataAdapter(selectCommand);
            DataSet dataSet = new DataSet();
            adapter.Fill(dataSet);
            return dataSet;
        }
        #endregion
        public int ExecuteNonQuery(string sql)
        {
            int num;
            OracleConnection connection = this.GetConnection();
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

        public static int ExecuteNonQuery(string sql, OracleConnection connection)
        {
            return ExecuteNonQuery(sql, CommandType.Text, connection, null, null);
        }

        public int ExecuteNonQuery(string sql, CommandType commandType)
        {
            int num;
            OracleConnection connection = this.GetConnection();
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
            OracleConnection connection = this.GetConnection();
            OracleTransaction transaction = null;
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

        public int ExecuteNonQuery(string sql, OracleParameter[] parameters)
        {
            int num;
            OracleConnection connection = this.GetConnection();
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

        public static int ExecuteNonQuery(string sql, OracleConnection connection, OracleTransaction transaction)
        {
            return ExecuteNonQuery(sql, CommandType.Text, connection, transaction, null);
        }

        public static int ExecuteNonQuery(string sql, OracleConnection connection, OracleParameter[] parameters)
        {
            return ExecuteNonQuery(sql, CommandType.Text, connection, null, parameters);
        }

        public static int ExecuteNonQuery(string sql, CommandType commandType, OracleConnection connection)
        {
            return ExecuteNonQuery(sql, commandType, connection, null, null);
        }

        public int ExecuteNonQuery(string sql, CommandType commandType, IsolationLevel iso)
        {
            int num2;
            OracleConnection connection = this.GetConnection();
            OracleTransaction transaction = null;
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

        public int ExecuteNonQuery(string sql, CommandType commandType, OracleParameter[] parameters)
        {
            int num;
            OracleConnection connection = this.GetConnection();
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

        public int ExecuteNonQuery(string sql, OracleParameter[] parameters, IsolationLevel iso)
        {
            int num2;
            OracleConnection connection = this.GetConnection();
            OracleTransaction transaction = null;
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

        public static int ExecuteNonQuery(string sql, OracleConnection connection, OracleTransaction transaction, OracleParameter[] parameters)
        {
            return ExecuteNonQuery(sql, CommandType.Text, connection, transaction, parameters);
        }

        public static int ExecuteNonQuery(string sql, CommandType commandType, OracleConnection connection, OracleTransaction transaction)
        {
            return ExecuteNonQuery(sql, commandType, connection, transaction, null);
        }

        public static int ExecuteNonQuery(string sql, CommandType commandType, OracleConnection connection, OracleParameter[] parameters)
        {
            return ExecuteNonQuery(sql, commandType, connection, null, parameters);
        }

        public int ExecuteNonQuery(string sql, CommandType commandType, OracleParameter[] parameters, IsolationLevel iso)
        {
            int num2;
            OracleConnection connection = this.GetConnection();
            OracleTransaction transaction = null;
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

        public static int ExecuteNonQuery(string sql, CommandType commandType, OracleConnection connection, OracleTransaction transaction, OracleParameter[] parameters)
        {
            OracleCommand command = new OracleCommand
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

        public static OracleDataReader ExecuteReader(string sql, OracleConnection connection)
        {
            return ExecuteReader(sql, CommandType.Text, connection, null, null);
        }

        public static OracleDataReader ExecuteReader(string sql, OracleConnection connection, OracleTransaction transaction)
        {
            return ExecuteReader(sql, CommandType.Text, connection, transaction, null);
        }

        public static OracleDataReader ExecuteReader(string sql, OracleConnection connection, OracleParameter[] parameters)
        {
            return ExecuteReader(sql, CommandType.Text, connection, null, parameters);
        }

        public static OracleDataReader ExecuteReader(string sql, CommandType commandType, OracleConnection connection)
        {
            return ExecuteReader(sql, commandType, connection, null, null);
        }

        public static OracleDataReader ExecuteReader(string sql, OracleConnection connection, OracleTransaction transaction, OracleParameter[] parameters)
        {
            return ExecuteReader(sql, CommandType.Text, connection, transaction, parameters);
        }

        public static OracleDataReader ExecuteReader(string sql, CommandType commandType, OracleConnection connection, OracleTransaction transaction)
        {
            return ExecuteReader(sql, commandType, connection, transaction, null);
        }

        public static OracleDataReader ExecuteReader(string sql, CommandType commandType, OracleConnection connection, OracleParameter[] parameters)
        {
            return ExecuteReader(sql, commandType, connection, null, parameters);
        }

        public static OracleDataReader ExecuteReader(string sql, CommandType commandType, OracleConnection connection, OracleTransaction transaction, OracleParameter[] parameters)
        {
            OracleCommand command = new OracleCommand
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

        public object ExecuteScalar(string sql)
        {
            object obj2;
            OracleConnection connection = this.GetConnection();
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

        public static object ExecuteScalar(string sql, OracleConnection connection)
        {
            return ExecuteScalar(sql, CommandType.Text, connection, null, null);
        }

        public object ExecuteScalar(string sql, CommandType commandType)
        {
            object obj2;
            OracleConnection connection = this.GetConnection();
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
            OracleConnection connection = this.GetConnection();
            OracleTransaction transaction = null;
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

        public object ExecuteScalar(string sql, OracleParameter[] parameters)
        {
            object obj2;
            OracleConnection connection = this.GetConnection();
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

        public static object ExecuteScalar(string sql, OracleConnection connection, OracleTransaction transaction)
        {
            return ExecuteScalar(sql, CommandType.Text, connection, transaction, null);
        }

        public static object ExecuteScalar(string sql, OracleConnection connection, OracleParameter[] parameters)
        {
            return ExecuteScalar(sql, CommandType.Text, connection, null, parameters);
        }

        public static object ExecuteScalar(string sql, CommandType commandType, OracleConnection connection)
        {
            return ExecuteScalar(sql, commandType, connection, null, null);
        }

        public object ExecuteScalar(string sql, CommandType commandType, IsolationLevel iso)
        {
            object obj3;
            OracleConnection connection = this.GetConnection();
            OracleTransaction transaction = null;
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

        public object ExecuteScalar(string sql, CommandType commandType, OracleParameter[] parameters)
        {
            object obj2;
            OracleConnection connection = this.GetConnection();
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

        public object ExecuteScalar(string sql, OracleParameter[] parameters, IsolationLevel iso)
        {
            object obj3;
            OracleConnection connection = this.GetConnection();
            OracleTransaction transaction = null;
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

        public static object ExecuteScalar(string sql, OracleConnection connection, OracleTransaction transaction, OracleParameter[] parameters)
        {
            return ExecuteScalar(sql, CommandType.Text, connection, transaction, parameters);
        }

        public static object ExecuteScalar(string sql, CommandType commandType, OracleConnection connection, OracleTransaction transaction)
        {
            return ExecuteScalar(sql, commandType, connection, transaction, null);
        }

        public static object ExecuteScalar(string sql, CommandType commandType, OracleConnection connection, OracleParameter[] parameters)
        {
            return ExecuteScalar(sql, commandType, connection, null, parameters);
        }

        public object ExecuteScalar(string sql, CommandType commandType, OracleParameter[] parameters, IsolationLevel iso)
        {
            object obj3;
            OracleConnection connection = this.GetConnection();
            OracleTransaction transaction = null;
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

        public static object ExecuteScalar(string sql, CommandType commandType, OracleConnection connection, OracleTransaction transaction, OracleParameter[] parameters)
        {
            OracleCommand command = new OracleCommand
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

        public static bool GetBoolean(OracleDataReader reader, int index)
        {
            return (!(reader[index] is DBNull) && Convert.ToBoolean(reader[index]));
        }

        public static bool GetBoolean(OracleDataReader reader, string name)
        {
            return (!(reader[name] is DBNull) && Convert.ToBoolean(reader[name]));
        }


        public static DateTime? GetDateTime(OracleDataReader reader, int index)
        {
            return ((reader[index] is DBNull) ? null : new DateTime?(Convert.ToDateTime(reader[index])));
        }

        public static DateTime? GetDateTime(OracleDataReader reader, string name)
        {
            return ((reader[name] is DBNull) ? null : new DateTime?(Convert.ToDateTime(reader[name])));
        }

        public static int? GetInt32(OracleDataReader reader, int index)
        {
            return ((reader[index] is DBNull) ? null : new int?(Convert.ToInt32(reader[index])));
        }

        public static int? GetInt32(OracleDataReader reader, string name)
        {
            return ((reader[name] is DBNull) ? null : new int?(Convert.ToInt32(reader[name])));
        }


        public static object TranslateNullToDBNull(object obj)
        {
            return ((obj == null) ? DBNull.Value : obj);
        }


    }
}
