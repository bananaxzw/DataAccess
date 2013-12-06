/********************************************************************************************
 * 设计人员:     bananaxzw@qq.com
 * 功能描述:     sql操作帮助                     
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
using System.Data.SqlClient;
using System.Data;
using System.Collections;

namespace Calvin.Data
{
    public class SqlOperation
    {
        private readonly string _connectionString;



        public SqlOperation(string connectionString)
        {

            _connectionString = connectionString;
        }

        public SqlConnection GetSqlConnection()
        {
            SqlConnection conn = new SqlConnection();
            conn.ConnectionString = _connectionString;
            conn.Open();
            return conn;
        }

        public void RecycleSqlConnection(SqlConnection connection)
        {
            if (connection == null)
            {
                return;
            }
            connection.Close();
            connection.Dispose();
        }


        #region ExecuteNonQuery

        public int ExecuteNonQuery(string sql)
        {
            SqlConnection conn = GetSqlConnection();
            try
            {
                return ExecuteNonQuery(sql, CommandType.Text, conn, null, null);
            }
            finally
            {
                RecycleSqlConnection(conn);
            }
        }

        public int ExecuteNonQuery(string sql, CommandType commandType)
        {
            SqlConnection conn = GetSqlConnection();
            try
            {
                return ExecuteNonQuery(sql, commandType, conn, null, null);
            }
            finally
            {
                RecycleSqlConnection(conn);
            }
        }

        public int ExecuteNonQuery(string sql, IsolationLevel iso)
        {
            SqlConnection conn = GetSqlConnection();
            SqlTransaction transaction = null;
            try
            {
                transaction = conn.BeginTransaction(iso);
                int result = ExecuteNonQuery(sql, CommandType.Text, conn, transaction, null);
                transaction.Commit();
                return result;
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
                RecycleSqlConnection(conn);
            }
        }

        public int ExecuteNonQuery(string sql, CommandType commandType, IsolationLevel iso)
        {
            SqlConnection conn = GetSqlConnection();
            SqlTransaction transaction = null;
            try
            {
                transaction = conn.BeginTransaction(iso);
                int result = ExecuteNonQuery(sql, commandType, conn, transaction, null);
                transaction.Commit();
                return result;
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
                RecycleSqlConnection(conn);
            }
        }

        public int ExecuteNonQuery(string sql, SqlConnection connection, SqlTransaction transaction)
        {
            return ExecuteNonQuery(sql, CommandType.Text, connection, transaction, null);
        }

        public int ExecuteNonQuery(string sql, CommandType commandType, SqlConnection connection, SqlTransaction transaction)
        {
            return ExecuteNonQuery(sql, commandType, connection, transaction, null);
        }


        public int ExecuteNonQuery(string sql, SqlParameter[] parameters)
        {
            SqlConnection conn = GetSqlConnection();
            try
            {
                return ExecuteNonQuery(sql, CommandType.Text, conn, null, parameters);
            }
            finally
            {
                RecycleSqlConnection(conn);
            }
        }

        public int ExecuteNonQuery(string sql, CommandType commandType, SqlParameter[] parameters)
        {
            SqlConnection conn = GetSqlConnection();
            try
            {
                return ExecuteNonQuery(sql, commandType, conn, null, parameters);
            }
            finally
            {
                RecycleSqlConnection(conn);
            }
        }

        public int ExecuteNonQuery(string sql, SqlParameter[] parameters, IsolationLevel iso)
        {
            SqlConnection conn = GetSqlConnection();
            SqlTransaction transaction = null;
            try
            {
                transaction = conn.BeginTransaction(iso);
                int result = ExecuteNonQuery(sql, CommandType.Text, conn, transaction, parameters);
                transaction.Commit();
                return result;
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
                RecycleSqlConnection(conn);
            }
        }

        public int ExecuteNonQuery(string sql, CommandType commandType, SqlParameter[] parameters, IsolationLevel iso)
        {
            SqlConnection conn = GetSqlConnection();
            SqlTransaction transaction = null;
            try
            {
                transaction = conn.BeginTransaction(iso);
                int result = ExecuteNonQuery(sql, commandType, conn, transaction, parameters);
                transaction.Commit();
                return result;
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
                RecycleSqlConnection(conn);
            }
        }

        public int ExecuteNonQuery(string sql, SqlConnection connection, SqlTransaction transaction, SqlParameter[] parameters)
        {
            return ExecuteNonQuery(sql, CommandType.Text, connection, transaction, parameters);
        }

        public int ExecuteNonQuery(string sql, CommandType commandType, SqlConnection connection, SqlTransaction transaction, SqlParameter[] parameters)
        {
            SqlCommand comm = new SqlCommand();
            comm.Connection = connection;
            comm.CommandText = sql;
            comm.CommandType = commandType;
            comm.Transaction = transaction;
            if (parameters != null &&
                parameters.Length > 0)
            {
                comm.Parameters.AddRange(parameters);
            }
            return comm.ExecuteNonQuery();
        }

        #endregion

        /**/
        /// <summary>
        ///批量执行SQL语句 
        /// </summary>
        /// <param name="SqlList">SQL列表</param>
        /// <returns></returns>
        public bool ExecuteCommand(ArrayList SqlList)
        {
            SqlConnection con = GetSqlConnection();
            bool iserror = false;
            string strerror = "";
            SqlTransaction SqlTran = con.BeginTransaction();
            try
            {
                for (int i = 0; i < SqlList.Count; i++)
                {

                    SqlCommand _command = new SqlCommand();
                    _command.Connection = con;
                    _command.CommandText = SqlList[i].ToString();
                    _command.Transaction = SqlTran;
                    _command.ExecuteNonQuery();
                }

            }
            catch (Exception ex)
            {
                iserror = true;
                strerror = ex.Message;

            }
            finally
            {

                if (iserror)
                {
                    SqlTran.Rollback();
                    throw new Exception(strerror);
                }
                else
                {
                    SqlTran.Commit();
                }
                con.Close();
            }
            if (iserror)
            {
                return false;
            }
            else
            {
                return true;
            }
        }


        #region ExecuteReader

        public SqlDataReader ExecuteReader(string sql, SqlConnection connection, SqlTransaction transaction)
        {
            return ExecuteReader(sql, CommandType.Text, connection, transaction, null);
        }

        public SqlDataReader ExecuteReader(string sql, CommandType commandType, SqlConnection connection, SqlTransaction transaction)
        {
            return ExecuteReader(sql, commandType, connection, transaction, null);
        }


        public SqlDataReader ExecuteReader(string sql, SqlConnection connection, SqlTransaction transaction, SqlParameter[] parameters)
        {
            return ExecuteReader(sql, CommandType.Text, connection, transaction, parameters);
        }

        public SqlDataReader ExecuteReader(string sql, CommandType commandType, SqlConnection connection, SqlTransaction transaction, SqlParameter[] parameters)
        {
            SqlCommand comm = new SqlCommand();
            comm.Connection = connection;
            comm.CommandText = sql;
            comm.CommandType = commandType;
            comm.Transaction = transaction;
            if (parameters != null &&
                parameters.Length > 0)
            {
                comm.Parameters.AddRange(parameters);
            }
            return comm.ExecuteReader();
        }

        #endregion


        #region ExecuteScalar

        public object ExecuteScalar(string sql)
        {
            SqlConnection conn = GetSqlConnection();
            try
            {
                return ExecuteScalar(sql, CommandType.Text, conn, null, null);
            }
            finally
            {
                RecycleSqlConnection(conn);
            }
        }

        public object ExecuteScalar(string sql, CommandType commandType)
        {
            SqlConnection conn = GetSqlConnection();
            try
            {
                return ExecuteScalar(sql, commandType, conn, null, null);
            }
            finally
            {
                RecycleSqlConnection(conn);
            }
        }

        public object ExecuteScalar(string sql, IsolationLevel iso)
        {
            SqlConnection conn = GetSqlConnection();
            SqlTransaction transaction = null;
            try
            {
                transaction = conn.BeginTransaction(iso);
                object result = ExecuteScalar(sql, CommandType.Text, conn, transaction, null);
                transaction.Commit();
                return result;
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
                RecycleSqlConnection(conn);
            }
        }

        public object ExecuteScalar(string sql, CommandType commandType, IsolationLevel iso)
        {
            SqlConnection conn = GetSqlConnection();
            SqlTransaction transaction = null;
            try
            {
                transaction = conn.BeginTransaction(iso);
                object result = ExecuteScalar(sql, commandType, conn, transaction, null);
                transaction.Commit();
                return result;
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
                RecycleSqlConnection(conn);
            }
        }

        public object ExecuteScalar(string sql, SqlConnection connection, SqlTransaction transaction)
        {
            return ExecuteScalar(sql, CommandType.Text, connection, transaction, null);
        }

        public object ExecuteScalar(string sql, CommandType commandType, SqlConnection connection, SqlTransaction transaction)
        {
            return ExecuteScalar(sql, commandType, connection, transaction, null);
        }


        public object ExecuteScalar(string sql, SqlParameter[] parameters)
        {
            SqlConnection conn = GetSqlConnection();
            try
            {
                return ExecuteScalar(sql, CommandType.Text, conn, null, parameters);
            }
            finally
            {
                RecycleSqlConnection(conn);
            }
        }

        public object ExecuteScalar(string sql, CommandType commandType, SqlParameter[] parameters)
        {
            SqlConnection conn = GetSqlConnection();
            try
            {
                return ExecuteScalar(sql, commandType, conn, null, parameters);
            }
            finally
            {
                RecycleSqlConnection(conn);
            }
        }

        public object ExecuteScalar(string sql, SqlParameter[] parameters, IsolationLevel iso)
        {
            SqlConnection conn = GetSqlConnection();
            SqlTransaction transaction = null;
            try
            {
                transaction = conn.BeginTransaction(iso);
                object result = ExecuteScalar(sql, CommandType.Text, conn, transaction, parameters);
                transaction.Commit();
                return result;
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
                RecycleSqlConnection(conn);
            }
        }

        public object ExecuteScalar(string sql, CommandType commandType, SqlParameter[] parameters, IsolationLevel iso)
        {
            SqlConnection conn = GetSqlConnection();
            SqlTransaction transaction = null;
            try
            {
                transaction = conn.BeginTransaction(iso);
                object result = ExecuteScalar(sql, commandType, conn, transaction, parameters);
                transaction.Commit();
                return result;
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
                RecycleSqlConnection(conn);
            }
        }

        public object ExecuteScalar(string sql, SqlConnection connection, SqlTransaction transaction, SqlParameter[] parameters)
        {
            return ExecuteScalar(sql, CommandType.Text, connection, transaction, parameters);
        }

        public object ExecuteScalar(string sql, CommandType commandType, SqlConnection connection, SqlTransaction transaction, SqlParameter[] parameters)
        {
            SqlCommand comm = new SqlCommand();
            comm.Connection = connection;
            comm.CommandText = sql;
            comm.CommandType = commandType;
            comm.Transaction = transaction;
            if (parameters != null &&
                parameters.Length > 0)
            {
                comm.Parameters.AddRange(parameters);
            }
            return comm.ExecuteScalar();
        }

        #endregion


        #region ExecuteDataSet

        public DataSet ExecuteDataSet(string sql)
        {
            SqlConnection conn = GetSqlConnection();
            try
            {
                return ExecuteDataSet(sql, CommandType.Text, conn, null, null);
            }
            finally
            {
                RecycleSqlConnection(conn);
            }
        }

        public DataSet ExecuteDataSet(string sql, CommandType commandType)
        {
            SqlConnection conn = GetSqlConnection();
            try
            {
                return ExecuteDataSet(sql, commandType, conn, null, null);
            }
            finally
            {
                RecycleSqlConnection(conn);
            }
        }

        public DataSet ExecuteDataSet(string sql, IsolationLevel iso)
        {
            SqlConnection conn = GetSqlConnection();
            SqlTransaction transaction = null;
            try
            {
                transaction = conn.BeginTransaction(iso);
                DataSet result = ExecuteDataSet(sql, CommandType.Text, conn, transaction, null);
                transaction.Commit();
                return result;
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
                RecycleSqlConnection(conn);
            }
        }

        public DataSet ExecuteDataSet(string sql, CommandType commandType, IsolationLevel iso)
        {
            SqlConnection conn = GetSqlConnection();
            SqlTransaction transaction = null;
            try
            {
                transaction = conn.BeginTransaction(iso);
                DataSet result = ExecuteDataSet(sql, commandType, conn, transaction, null);
                transaction.Commit();
                return result;
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
                RecycleSqlConnection(conn);
            }
        }

        public DataSet ExecuteDataSet(string sql, SqlConnection connection, SqlTransaction transaction)
        {
            return ExecuteDataSet(sql, CommandType.Text, connection, transaction, null);
        }

        public DataSet ExecuteDataSet(string sql, CommandType commandType, SqlConnection connection, SqlTransaction transaction)
        {
            return ExecuteDataSet(sql, commandType, connection, transaction, null);
        }


        public DataSet ExecuteDataSet(string sql, SqlParameter[] parameters)
        {
            SqlConnection conn = GetSqlConnection();
            try
            {
                return ExecuteDataSet(sql, CommandType.Text, conn, null, parameters);
            }
            finally
            {
                RecycleSqlConnection(conn);
            }
        }

        public DataSet ExecuteDataSet(string sql, CommandType commandType, SqlParameter[] parameters)
        {
            SqlConnection conn = GetSqlConnection();
            try
            {
                return ExecuteDataSet(sql, commandType, conn, null, parameters);
            }
            finally
            {
                RecycleSqlConnection(conn);
            }
        }

        public DataSet ExecuteDataSet(string sql, SqlParameter[] parameters, IsolationLevel iso)
        {
            SqlConnection conn = GetSqlConnection();
            SqlTransaction transaction = null;
            try
            {
                transaction = conn.BeginTransaction(iso);
                DataSet result = ExecuteDataSet(sql, CommandType.Text, conn, transaction, parameters);
                transaction.Commit();
                return result;
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
                RecycleSqlConnection(conn);
            }
        }

        public DataSet ExecuteDataSet(string sql, CommandType commandType, SqlParameter[] parameters, IsolationLevel iso)
        {
            SqlConnection conn = GetSqlConnection();
            SqlTransaction transaction = null;
            try
            {
                transaction = conn.BeginTransaction(iso);
                DataSet result = ExecuteDataSet(sql, commandType, conn, transaction, parameters);
                transaction.Commit();
                return result;
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
                RecycleSqlConnection(conn);
            }
        }

        public DataSet ExecuteDataSet(string sql, SqlConnection connection, SqlTransaction transaction, SqlParameter[] parameters)
        {
            return ExecuteDataSet(sql, CommandType.Text, connection, transaction, parameters);
        }

        public DataSet ExecuteDataSet(string sql, CommandType commandType, SqlConnection connection, SqlTransaction transaction, SqlParameter[] parameters)
        {
            SqlCommand comm = new SqlCommand();
            comm.Connection = connection;
            comm.CommandText = sql;
            comm.CommandType = commandType;
            comm.Transaction = transaction;
            if (parameters != null &&
                parameters.Length > 0)
            {
                comm.Parameters.AddRange(parameters);
            }
            SqlDataAdapter adapter = new SqlDataAdapter(comm);
            DataSet result = new DataSet();
            adapter.Fill(result);
            return result;
        }

        #endregion
    }
}
