using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Oracle.DataAccess.Client;
using System.Configuration;
using System.Data;

namespace AnonymizedNamespace
{
    class AnonymizedProgram
    {
        static int Main(string[] args)
        {
            int exitCode;
            string environment = "";
            if (args.Length > 0)
                environment = args[0];

            Console.WriteLine("Processing " + environment + " environment.");
            exitCode = Run(environment);

            return exitCode;
        }

        static int Run(string environment)
        {
            int exitCode = 0;
            try
            {
                if (string.IsNullOrEmpty(environment))
                {
                    Console.WriteLine("Error: Environment parameter is required.");
                    return 1;
                }

                DataTable deployData = GetDeploymentData(environment);

                foreach (DataRow row in deployData.Rows)
                {
                    try
                    {
                        string result;
                        string configIdType = row["CONFIG_ID_TYPE"]?.ToString() ?? "";
                        string configId = row["CONFIG_ID"]?.ToString() ?? "";
                        string rowId = row["id"]?.ToString() ?? "";

                        if (string.IsNullOrEmpty(configIdType) || string.IsNullOrEmpty(configId) || string.IsNullOrEmpty(rowId))
                        {
                            UpdateDeploymentTable(environment, rowId, "Invalid configuration data");
                            continue;
                        }

                        switch (configIdType)
                        {
                            case "F":
                                result = ProcessDeployment(GetNextEnv(environment), "ANONYMIZED_SCHEMA.ANONYMIZED_PROC_FORM", configId, "I_FORM_ID");
                                UpdateDeploymentTable(environment, rowId, result);
                                break;
                            case "R":
                                result = ProcessDeployment(GetNextEnv(environment), "ANONYMIZED_SCHEMA.ANONYMIZED_PROC_REPORT", configId, "I_REPORT_ID");
                                UpdateDeploymentTable(environment, rowId, result);
                                break;
                            case "A":
                                result = ProcessDeployment(GetNextEnv(environment), "ANONYMIZED_SCHEMA.ANONYMIZED_PROC_APP", configId, "I_APP_ID");
                                UpdateDeploymentTable(environment, rowId, result);
                                break;
                            case "P":
                                result = ProcessPageDeployment(GetNextEnv(environment), environment, configId);
                                UpdateDeploymentTable(environment, rowId, result);
                                break;
                            case "I":
                                result = ProcessIntegrityCheckDeployment(GetNextEnv(environment), environment, configId);
                                UpdateDeploymentTable(environment, rowId, result);
                                break;
                            default:
                                UpdateDeploymentTable(environment, rowId, "Not a Valid config type");
                                break;
                        }
                    }
                    catch (Exception ex)
                    {
                        string rowId = row["id"]?.ToString() ?? "";
                        UpdateDeploymentTable(environment, rowId, $"Error processing deployment: {ex.Message}");
                        Console.WriteLine($"Error processing row {rowId}: {ex.Message}");
                    }
                }
            }
            catch (Exception ex)
            {
                Console.WriteLine($"Critical error: {ex.Message}");
                Console.WriteLine(ex.StackTrace);
                exitCode = 1;
            }

            return exitCode;
        }

        private static string ProcessIntegrityCheckDeployment(string nextEnv, string environment, string id)
        {
            try
            {
                if (!int.TryParse(id, out int checkNum))
                {
                    return "Invalid check number format";
                }

                ExecuteSqlWithParameter(nextEnv, "delete from anonymized_integrity_check where check_num = :checkNum", 
                    new OracleParameter("checkNum", OracleDbType.Int32) { Value = checkNum });
                
                ExecuteSqlWithParameter(nextEnv, "insert into anonymized_integrity_check select * from anonymized_integrity_check@deploy where check_num = :checkNum", 
                    new OracleParameter("checkNum", OracleDbType.Int32) { Value = checkNum });
                
                return "Processed without .NET errors";
            }
            catch (Exception ex)
            {
                return $"Error processing integrity check deployment: {ex.Message}";
            }
        }

        static string ProcessPageDeployment(string nextEnv, string environment, string id)
        {
            try
            {
                if (string.IsNullOrEmpty(id))
                {
                    return "Invalid page ID";
                }

                try
                {
                    ExecuteSql(nextEnv, "drop table anonymized_page_bk");
                }
                catch (Exception ex)
                {
                    Console.WriteLine($"Warning dropping anonymized_page_bk: {ex.Message}");
                }
                ExecuteSql(nextEnv, "create table anonymized_page_bk as select * from anonymized_page");

                try
                {
                    ExecuteSql(nextEnv, "drop table anonymized_page_nav_tree_bk");
                }
                catch (Exception ex)
                {
                    Console.WriteLine($"Warning dropping anonymized_page_nav_tree_bk: {ex.Message}");
                }
                ExecuteSql(nextEnv, "create table anonymized_page_nav_tree_bk as select * from anonymized_page_nav_tree");

                try
                {
                    ExecuteSql(nextEnv, "drop table anonymized_page_security_bk");
                }
                catch (Exception ex)
                {
                    Console.WriteLine($"Warning dropping anonymized_page_security_bk: {ex.Message}");
                }
                ExecuteSql(nextEnv, "create table anonymized_page_security_bk as select * from anonymized_page_security");

                ExecuteSqlWithParameter(nextEnv, "delete from anonymized_page where page_id = :pageId", 
                    new OracleParameter("pageId", OracleDbType.Varchar2) { Value = id });
                ExecuteSqlWithParameter(nextEnv, "insert into anonymized_page select * from anonymized_page@deploy where page_id = :pageId", 
                    new OracleParameter("pageId", OracleDbType.Varchar2) { Value = id });

                ExecuteSqlWithParameter(nextEnv, "delete from anonymized_page_nav_tree where page_id = :pageId", 
                    new OracleParameter("pageId", OracleDbType.Varchar2) { Value = id });
                ExecuteSqlWithParameter(nextEnv, "insert into anonymized_page_nav_tree select * from anonymized_page_nav_tree@deploy where page_id = :pageId", 
                    new OracleParameter("pageId", OracleDbType.Varchar2) { Value = id });

                ExecuteSqlWithParameter(nextEnv, "delete from anonymized_page_security where page_id = :pageId", 
                    new OracleParameter("pageId", OracleDbType.Varchar2) { Value = id });
                ExecuteSqlWithParameter(nextEnv, "insert into anonymized_page_security select * from anonymized_page_security@deploy where page_id = :pageId", 
                    new OracleParameter("pageId", OracleDbType.Varchar2) { Value = id });

                return "Processed without .NET errors";
            }
            catch (Exception ex)
            {
                return $"Error processing page deployment: {ex.Message}";
            }
        }

        static void ExecuteSql(string nextEnv, string sql)
        {
            try
            {
                using (OracleConnection conn = new OracleConnection())
                {
                    conn.ConnectionString = ConfigurationManager.AppSettings[nextEnv]?.ToString();
                    if (string.IsNullOrEmpty(conn.ConnectionString))
                    {
                        throw new InvalidOperationException($"Connection string not found for environment: {nextEnv}");
                    }
                    
                    conn.Open();
                    using (OracleCommand cmd = new OracleCommand(sql, conn))
                    {
                        cmd.ExecuteNonQuery();
                    }
                }
            }
            catch (Exception ex)
            {
                throw new Exception($"Error executing SQL '{sql}': {ex.Message}", ex);
            }
        }

        static void ExecuteSqlWithParameter(string nextEnv, string sql, OracleParameter parameter)
        {
            try
            {
                using (OracleConnection conn = new OracleConnection())
                {
                    conn.ConnectionString = ConfigurationManager.AppSettings[nextEnv]?.ToString();
                    if (string.IsNullOrEmpty(conn.ConnectionString))
                    {
                        throw new InvalidOperationException($"Connection string not found for environment: {nextEnv}");
                    }
                    
                    conn.Open();
                    using (OracleCommand cmd = new OracleCommand(sql, conn))
                    {
                        cmd.Parameters.Add(parameter);
                        cmd.ExecuteNonQuery();
                    }
                }
            }
            catch (Exception ex)
            {
                throw new Exception($"Error executing parameterized SQL '{sql}': {ex.Message}", ex);
            }
        }

        static string ProcessDeployment(string nextEnv, string proc, string id, string variable)
        {
            try
            {
                if (!int.TryParse(id, out int configId))
                {
                    return "Invalid configuration ID format";
                }

                string result;
                using (OracleConnection conn = new OracleConnection())
                {
                    conn.ConnectionString = ConfigurationManager.AppSettings[nextEnv]?.ToString();
                    if (string.IsNullOrEmpty(conn.ConnectionString))
                    {
                        throw new InvalidOperationException($"Connection string not found for environment: {nextEnv}");
                    }
                    
                    conn.Open();
                    using (OracleCommand cmd = new OracleCommand(proc, conn))
                    {
                        cmd.CommandType = CommandType.StoredProcedure;
                        cmd.Parameters.Add("I_MIGRATION_DESC", OracleDbType.Varchar2).Value = "auto deployment for " + configId;
                        cmd.Parameters.Add(variable, OracleDbType.Int32).Value = configId;
                        cmd.ExecuteNonQuery();
                    }
                }
                result = "Processed without .NET errors";
                return result;
            }
            catch (Exception ex)
            {
                return $"Error processing deployment: {ex.Message}";
            }
        }

        static void UpdateDeploymentTable(string environment, string rowId, string result)
        {
            try
            {
                if (!int.TryParse(rowId, out int id))
                {
                    Console.WriteLine($"Invalid row ID format: {rowId}");
                    return;
                }

                string sql = "UPDATE ANONYMIZED_DEPLOY_CONFIG SET DEPLOY_DATE = sysdate, ERROR_MSG = :errorMsg WHERE id = :rowId";
                using (OracleConnection conn = new OracleConnection())
                {
                    conn.ConnectionString = ConfigurationManager.AppSettings[environment]?.ToString();
                    if (string.IsNullOrEmpty(conn.ConnectionString))
                    {
                        throw new InvalidOperationException($"Connection string not found for environment: {environment}");
                    }
                    
                    conn.Open();
                    using (OracleCommand cmd = new OracleCommand(sql, conn))
                    {
                        cmd.Parameters.Add("errorMsg", OracleDbType.Varchar2).Value = result ?? "";
                        cmd.Parameters.Add("rowId", OracleDbType.Int32).Value = id;
                        cmd.ExecuteNonQuery();
                    }
                }

                if (environment == "Test")
                {
                    UpdatePreProdDeploymentTable(GetNextEnv(environment), rowId);
                }
            }
            catch (Exception ex)
            {
                Console.WriteLine($"Error updating deployment table: {ex.Message}");
            }
        }

        static void UpdatePreProdDeploymentTable(string nextEnv, string rowId)
        {
            try
            {
                if (!int.TryParse(rowId, out int id))
                {
                    Console.WriteLine($"Invalid row ID format: {rowId}");
                    return;
                }

                string sql = "INSERT INTO ANONYMIZED_DEPLOY_CONFIG ( CONFIG_ID_TYPE, CONFIG_ID ) SELECT CONFIG_ID_TYPE, CONFIG_ID FROM ANONYMIZED_DEPLOY_CONFIG@deploy WHERE id = :rowId";
                using (OracleConnection conn = new OracleConnection())
                {
                    conn.ConnectionString = ConfigurationManager.AppSettings[nextEnv]?.ToString();
                    if (string.IsNullOrEmpty(conn.ConnectionString))
                    {
                        throw new InvalidOperationException($"Connection string not found for environment: {nextEnv}");
                    }
                    
                    conn.Open();
                    using (OracleCommand cmd = new OracleCommand(sql, conn))
                    {
                        cmd.Parameters.Add("rowId", OracleDbType.Int32).Value = id;
                        cmd.ExecuteNonQuery();
                    }
                }
            }
            catch (Exception ex)
            {
                Console.WriteLine($"Error updating pre-prod deployment table: {ex.Message}");
            }
        }

        static DataTable GetDeploymentData(string environment)
        {
            DataTable table = new DataTable();
            try
            {
                string sql = "SELECT * FROM ANONYMIZED_DEPLOY_CONFIG WHERE DEPLOY_DATE is null";
                using (OracleConnection conn = new OracleConnection())
                {
                    conn.ConnectionString = ConfigurationManager.AppSettings[environment]?.ToString();
                    if (string.IsNullOrEmpty(conn.ConnectionString))
                    {
                        throw new InvalidOperationException($"Connection string not found for environment: {environment}");
                    }
                    
                    conn.Open();
                    using (OracleCommand cmd = new OracleCommand(sql, conn))
                    {
                        using (OracleDataAdapter adapter = new OracleDataAdapter(cmd))
                        {
                            adapter.Fill(table);
                        }
                    }
                }
            }
            catch (Exception ex)
            {
                Console.WriteLine($"Error retrieving deployment data: {ex.Message}");
                throw;
            }
            return table;
        }

        static string GetNextEnv(string environment)
        {
            switch (environment)
            {
                case "Dev":
                    return "Test";
                case "Test":
                    return "PreProd";
                case "PreProd":
                    return "Prod";
                default:
                    return "Test";
            }
        }
    }
}
