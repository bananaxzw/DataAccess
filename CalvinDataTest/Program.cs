

using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Oracle.DataAccess.Client;
using Calvin.Data;
using System.Data;
using System.IO;
namespace CalvinDataTest
{
    class Program
    {

        static void Main(string[] args)
        {
         
            string _connstring = "Data Source=118orcl;User Id=epm;Password=epm;";
            OracleOperation op = new OracleOperation(_connstring);
            FileStream stream = File.OpenRead("c:\\system-conf.xml");
            Byte[] byr=new byte[stream.Length];
            stream.Read(byr, 0,Convert.ToInt32(stream.Length));
            try
            {
             
                OracleParameter[] parameters = { 
                        new OracleParameter("p_id", "1"),
                        new OracleParameter("p_name", "Mudguards"),
                        new OracleParameter("p_content",OracleDbType.Blob)

                                               };
                parameters[2].Value = byr;
               
                op.ExecuteNonQuery("testxxxx.PRO_Insert", CommandType.StoredProcedure, parameters);
                DataTable dt = op.ExecuteDataSet("select content from test where id='2'").Tables[0];

              string ss=  System.Text.Encoding.UTF8.GetString((Byte[])dt.Rows[0][0]);
               
            }

                 
            catch (Exception ex)
            {
               
            }
        }
    }
}
