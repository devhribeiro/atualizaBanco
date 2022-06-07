using System.Text.RegularExpressions;
using System;
using System.IO;
using System.Data.SqlClient;
using System.Linq;
using System.Collections.Generic;
using System.Data;
using Newtonsoft.Json.Linq;
using System.Threading.Tasks;
using Azure.Identity;
using Azure.Security.KeyVault.Secrets;

namespace AtualizacaoBanco
{
    class Program
    {
        static void Main(string[] args)
        {
            string connectionString = string.Empty;
            bool build = false;
            // Comentar para debugar e testar
            if (args == null || args.Length < 3 || !(args.Length == 3 || args.Length == 5 || args.Length == 6))
            {
                Console.WriteLine("----Manual de Utilização----");
                Console.WriteLine("\nPrimeiro método para utilização no DevOps:\nAtualizaBanco.exe [Application (client) ID] [Object ID] [Directory (tenant) ID] [keyVaultUrl] [keyVaultName] [Build ou Release]");
                Console.WriteLine("\nSegundo método utilização local com usuario e senha:\nAtualizaBanco.exe [Server] [Database] [User] [Password] [Build ou Release]");
                Console.WriteLine("\nTerceiro método local com usuario WINDOWS:\nAtualizaBanco.exe [Server] [Database] [Build ou Release]\n");
                throw new ArgumentException("Erro: Dados de parâmetros insuficientes...");
            }


            if (args.Length == 6)
            {
                string clientId = args[0].ToString();
                string clientSecret = args[1].ToString();
                string tenantId = args[2].ToString();
                string keyVaultUrl = args[3].ToString();
                string keyVaultName = args[4].ToString();
                build = args[5].ToString().ToUpper() == "BUILD" ? true : false;
                connectionString = ObterConectionString(clientId, clientSecret, tenantId, keyVaultUrl, keyVaultName).GetAwaiter().GetResult();
            }

            if (args.Length == 5)
            {

                var server = args[0].ToString();
                var database = args[1].ToString();
                var user = args[2].ToString();
                var pass = args[3].ToString();
                build = args[4].ToString().ToUpper() == "BUILD" ? true : false;
                connectionString = $"Server={server};Uid={user};Password={pass};Database={database};MultipleActiveResultSets=True;";

                System.Console.WriteLine("==== Processo iniciado com conexão local ====");
            }
            if (args.Length == 3)
            {
                var server = args[0].ToString();
                var database = args[1].ToString();
                build = args[2].ToString().ToUpper() == "BUILD" ? true : false;
                connectionString = $"Server={server};Database={database};MultipleActiveResultSets=True;Integrated Security=True;";

                System.Console.WriteLine("==== Processo iniciado com conexão local ====");
            }

            // Descomentar para debugar e testar
            //var build = false;
            //string connectionString = "Server=localhost;Uid=sa;Password=CodenApp10;Database=db-crefaz-dev;MultipleActiveResultSets=True;";

            System.Console.WriteLine("Iniciando processo...");
            System.Console.WriteLine("----------------------------------------------------------------");

            string caminhoArquivo = Directory.GetCurrentDirectory() + "/../Sql/";

            if (!Directory.Exists(caminhoArquivo))
                throw new Exception("O diretório Sql não existe: " + caminhoArquivo);

            using (SqlConnection connection = new SqlConnection(connectionString))
            {
                if (connection.State == ConnectionState.Closed) connection.Open();
                SqlTransaction transacao = connection.BeginTransaction("AlteracaoObjetoAppConsole");

                try
                {
                    PercorreDiretoriosAtualizando(caminhoArquivo, connection, transacao);

                    if (build)
                    {
                        System.Console.WriteLine("Iniciando rollback da transação...");
                        transacao.Rollback();
                        System.Console.WriteLine("Rollback da transação finalizado ...");
                    }
                    else
                    {
                        System.Console.WriteLine("Iniciando commit da transação...");
                        transacao.Commit();
                        System.Console.WriteLine("Commit da transação finalizado ...");
                    }

                }
                catch (Exception)
                {
                    System.Console.WriteLine("Iniciando rollback da transação...");
                    transacao.Rollback();
                    System.Console.WriteLine("Rollback da transação finalizado ...");
                    throw;
                }
                finally
                {
                    if (connection.State == ConnectionState.Open) connection.Close();
                }
            }

            System.Console.WriteLine("----------------------------------------------------------------");
            System.Console.WriteLine("Finalizando processo...");
        }

        private static async Task<string> ObterConectionString(string clientId, string clientSecret, string tenantId, string keyVaultUrl, string keyVaultName)
        {
            var client = new SecretClient(new Uri(keyVaultUrl), new ClientSecretCredential(tenantId, clientId, clientSecret));

            var secret = await client.GetSecretAsync(keyVaultName);

            return secret.Value.Value;
        }

        private static void PercorreDiretoriosAtualizando(
            string caminhoArquivo,
            SqlConnection connection,
            SqlTransaction transacao
        )
        {
            var pastasDiretorio = Directory.GetDirectories(caminhoArquivo);
            var pastas = RemoveDiretorios(pastasDiretorio);

            if (connection.State == ConnectionState.Closed) connection.Open();

            IEnumerable<string> pastasOrdenadas = from pasta in pastas
                                                  orderby pasta.Contains("Scripts")
                                                  orderby pasta.Contains("View")
                                                  orderby pasta.Contains("Scalar-valued Functions")
                                                  orderby pasta.Contains("Table-valued Functions")
                                                  orderby pasta.Contains("StoredProcedure")
                                                  select pasta;

            foreach (var pasta in pastasOrdenadas)
            {
                PercorreDiretoriosAtualizando(pasta, connection, transacao);
                AtualizarScripts(pasta, connection, transacao);
            }
        }

        private static string[] RemoveDiretorios(string[] pastas)
        {
            var lista = pastas.ToList();

            var json = System.IO.File.ReadAllText("IgnoreFolderOrFiles.json");

            var obj = JObject.Parse(json);

            foreach (var valor in obj["Pastas"].Children())
            {
                lista.RemoveAll(x => x.Contains(valor.ToString()));
            }

            return lista.ToArray();
        }

        private static void AtualizarScripts(
            string caminhoArquivo,
            SqlConnection connection,
            SqlTransaction transacao
        )
        {
            if (!Directory.Exists(caminhoArquivo))
                return;

            Regex rx = new Regex(@"\b(?:[^\\\/](?!(\\|\/)))+$\b", RegexOptions.Compiled | RegexOptions.IgnoreCase);
            var objetos = BuscaObjetosBanco(connection, transacao);
            var migracao = BuscaMigracao(connection, transacao);
            string[] arquivos = ListaArquivosPasta(caminhoArquivo);
            string nomeObjecto = string.Empty;

            if (rx.Match(caminhoArquivo).Value == "Scripts")
                ValidaNomeArquivoMigracaoForaDoPadrao(rx, migracao, arquivos);

            if (connection.State == ConnectionState.Closed) connection.Open();

            try
            {
                foreach (var arquivo in arquivos)
                {
                    if (rx.Match(caminhoArquivo).Value == "Scripts")
                    {
                        nomeObjecto = rx.Match(arquivo).Value;
                        if (!VerificaMigracaoEstaNoBanco(migracao, rx.Match(arquivo).Value))
                        {
                            var linhasQuery = System.IO.File.ReadAllLines(arquivo);
                            var query = string.Join(" \r\n ", linhasQuery);

                            if (query.Contains("�"))
                                throw new Exception($"erro no arquivo {rx.Match(arquivo).Value} contém codificação diferente de UTF8");

                            using (SqlCommand command = new SqlCommand(query, connection))
                            {
                                command.Transaction = transacao;
                                command.ExecuteNonQuery();
                                GravaMigracao(rx.Match(arquivo).Value, connection, transacao);
                            }

                            Console.WriteLine($"Pasta: {rx.Match(caminhoArquivo).Value} \n Arquivo Migração: {rx.Match(arquivo).Value}");
                        }
                        else
                        {
                            System.Console.WriteLine($"Já existe uma Migração do arquivo: {rx.Match(arquivo).Value}");
                        }

                    }
                    else
                    {
                        var linhasQuery = System.IO.File.ReadAllLines(arquivo);
                        nomeObjecto = RetornaNomeObjeto(linhasQuery);
                        var listaQuery = RemoveCaracteresEspeciais(linhasQuery);

                        var query = string.Join(" \r\n ", listaQuery);

                        if (query.Contains("�"))
                            throw new Exception($"erro no arquivo {rx.Match(arquivo).Value} contém codificação diferente de UTF8");

                        if (nomeObjecto != "")
                        {
                            if (VerificaObjetoEstaNoBanco(objetos, nomeObjecto))
                                query = AlteraScriptParaAlter(query);
                            else
                                query = AlteraScriptParaCreate(query);

                            using (SqlCommand command = new SqlCommand(query, connection))
                            {
                                command.Transaction = transacao;
                                command.ExecuteNonQuery();
                            }

                            Console.WriteLine($"Pasta: {rx.Match(caminhoArquivo).Value} Objeto alterado: {nomeObjecto}");
                        }
                    }

                }
            }
            catch (Exception ex)
            {
                Console.WriteLine($"Erro ao alterar o objeto {nomeObjecto} | Erro: {ex.Message}");
                throw;
            }
            finally { }
        }

        private static void ValidaNomeArquivoMigracaoForaDoPadrao(Regex regex, List<string> migracaoNoBanco, string[] arquivosNaPasta)
        {
            foreach (var arquivo in arquivosNaPasta)
            {
                var nomeArquivo = regex.Match(arquivo).Value;
                var prefixoData = nomeArquivo.Substring(0, 10);
                DateTime dateValue;

                if (!VerificaMigracaoEstaNoBanco(migracaoNoBanco, nomeArquivo))
                    if (!DateTime.TryParse(prefixoData, out dateValue))
                        throw new Exception($"O nome do arquivo de script {nomeArquivo} é inválido. O arquivo deve iniciar com a data de criação, no formato yyyy-mm-dd e recomenda-se incluir PBI-Task-Descrição para melhor organização. Ex: '2021-05-18 - 1063-1197-Criar controle de Situações.sql'");
            }
        }

        private static string[] ListaArquivosPasta(string caminhoArquivo)
        {
            var arquivosPasta = Directory.GetFiles(caminhoArquivo, "*.sql");
            var listaArquivos = arquivosPasta.ToList();

            var json = System.IO.File.ReadAllText("IgnoreFolderOrFiles.json");

            var obj = JObject.Parse(json);

            foreach (var valor in obj["Arquivos"].Children())
            {
                listaArquivos.RemoveAll(x => x.Contains(valor.ToString()));
            }

            return listaArquivos.ToArray();
        }

        private static void GravaMigracao(
            string Arquivo,
            SqlConnection connection,
            SqlTransaction transacao
        )
        {
            var query = $"INSERT INTO Migracao.Migracao (Arquivo, Criacao) VALUES ('{Arquivo}', '{TimeZoneInfo.ConvertTime(DateTime.Now, TimeZoneInfo.FindSystemTimeZoneById("E. South America Standard Time")).ToString("yyyyMMdd hh:mm:ss")}')";

            using (SqlCommand command = new SqlCommand(query, connection))
            {
                command.Transaction = transacao;
                command.ExecuteNonQuery();
            }
        }

        private static bool VerificaObjetoEstaNoBanco(List<string> objetos, string nomeObjecto)
        {
            return objetos.Contains(RetiraCaracteresEspeciais(nomeObjecto));
        }

        private static bool VerificaMigracaoEstaNoBanco(List<string> objetos, string nomeObjecto)
        {
            return objetos.Contains(nomeObjecto);
        }

        private static string RetiraCaracteresEspeciais(string palavra)
        {
            var pontoIndice = palavra.IndexOf(".");
            palavra = palavra.Substring(pontoIndice, palavra.Length - pontoIndice);

            string pattern = @"(?i)[^0-9a-záéíóúàèìòùâêîôûãõç\s_]";
            Regex rgx = new Regex(pattern);
            palavra = rgx.Replace(palavra, "");

            return palavra;
        }

        private static List<String> BuscaObjetosBanco(
            SqlConnection connection,
            SqlTransaction transacao
        )
        {
            var objetos = new List<String>();
            try
            {
                if (connection.State == ConnectionState.Closed) connection.Open();

                var queryExisteObjecto = $"SELECT name FROM sys.objects WITH(NOLOCK) WHERE type_desc IN ('SQL_STORED_PROCEDURE', 'SQL_SCALAR_FUNCTION', 'SQL_TABLE_VALUED_FUNCTION', 'VIEW')";

                using (SqlCommand command = new SqlCommand(queryExisteObjecto, connection))
                {
                    command.Transaction = transacao;
                    using (SqlDataReader reader = command.ExecuteReader())
                    {
                        while (reader.Read())
                            objetos.Add(reader["name"].ToString());
                    }
                }
            }
            catch (Exception ex)
            {
                Console.WriteLine("Erro ao buscar objetos: " + ex.Message);
                throw;
            }
            finally { }

            return objetos;
        }

        private static List<String> BuscaMigracao(
            SqlConnection connection,
            SqlTransaction transacao
        )
        {
            var objetos = new List<String>();
            try
            {

                if (connection.State == ConnectionState.Closed) connection.Open();

                var CreateSchema = "if not exists(select * from sys.schemas where name like 'Migracao')" +
                    "exec('create schema Migracao')";
                using (SqlCommand command = new SqlCommand(CreateSchema, connection))
                {
                    try
                    {
                        command.Transaction = transacao;
                        command.ExecuteNonQuery();
                    }
                    catch (Exception ex)
                    {
                        Console.WriteLine("Erro ao Schema tabela migracao: " + ex.Message);
                    }
                }

                var CriaMigracao = "if not exists (select *, SCHEMA_NAME(Schema_ID) from Sys.tables where name like 'Migracao' and SCHEMA_NAME(Schema_ID) = 'Migracao')" +
                    "create table Migracao.Migracao([Id] [int] IDENTITY(1,1) NOT NULL," +
                    "[Arquivo] [nvarchar](255) NOT NULL,[Criacao] [datetime] NOT NULL)";
                using (SqlCommand command = new SqlCommand(CriaMigracao, connection))
                {
                    try
                    {
                        command.Transaction = transacao;
                        command.ExecuteNonQuery();
                    }
                    catch (Exception ex)
                    {
                        Console.WriteLine("Erro ao Criar tabela migracao: " + ex.Message);
                    }
                }

                var queryMigracao = $"SELECT Arquivo, Criacao FROM Migracao.Migracao";

                using (SqlCommand command = new SqlCommand(queryMigracao, connection))
                {
                    command.Transaction = transacao;
                    using (SqlDataReader reader = command.ExecuteReader())
                    {
                        while (reader.Read())
                            objetos.Add(reader["Arquivo"].ToString());
                    }
                }
            }
            catch (Exception ex)
            {
                Console.WriteLine("Erro ao buscar migracao: " + ex.Message);
                throw;
            }
            finally { }

            return objetos;
        }

        private static String[] RemoveCaracteresEspeciais(
            String[] linhas
        )
        {
            var listLinhas = linhas.ToList();

            listLinhas.RemoveAll(x => ((string)x.ToUpper()) == "GO");
            listLinhas.RemoveAll(x => ((string)x.ToUpper()) == "SET ANSI_NULLS ON");
            listLinhas.RemoveAll(x => ((string)x.ToUpper()) == "SET QUOTED_IDENTIFIER ON");

            return listLinhas.ToArray();
        }

        private static string RetornaNomeObjeto(string[] linhas)
        {

            var metodo = RetornaMetodoObjeto(linhas);
            if (metodo != "")
            {
                var somaCaracteresIndice = metodo.Length;
                var linha = linhas.Where(x => x.Contains(metodo, StringComparison.InvariantCultureIgnoreCase)).FirstOrDefault();
                var indiceInicio = linha.IndexOf(metodo, StringComparison.InvariantCultureIgnoreCase) + metodo.Length;
                var indiceTamanhoColchetes = linha.IndexOf("]", linha.IndexOf("]") + 1) + 1;
                var indiceTamanhoEspaco = linha.IndexOf(" ", indiceInicio, StringComparison.InvariantCultureIgnoreCase);
                var indiceTamanho = (indiceTamanhoColchetes < 1) ? indiceTamanhoEspaco : indiceTamanhoColchetes;

                indiceTamanho = (indiceTamanho < 1) ? linha.Length : indiceTamanho;

                var nome = linha.Substring(indiceInicio, indiceTamanho - indiceInicio);

                return nome;
            }

            return "";
        }

        private static string AlteraScriptParaCreate(string query)
        {
            var createProcedure = "create procedure";
            var alterProcedure = "alter procedure";

            var createFunction = "create function";
            var alterFunction = "alter function";

            var createView = "create view";
            var alterView = "alter view";

            query = Regex.Replace(query, alterProcedure, createProcedure, RegexOptions.IgnoreCase);
            query = Regex.Replace(query, alterFunction, createFunction, RegexOptions.IgnoreCase);
            query = Regex.Replace(query, alterView, createView, RegexOptions.IgnoreCase);

            return query;
        }

        private static string AlteraScriptParaAlter(string query)
        {
            var createProcedure = "create procedure";
            var alterProcedure = "alter procedure";

            var createFunction = "create function";
            var alterFunction = "alter function";

            var createView = "create view";
            var alterView = "alter view";

            query = Regex.Replace(query, createProcedure, alterProcedure, RegexOptions.IgnoreCase);
            query = Regex.Replace(query, createFunction, alterFunction, RegexOptions.IgnoreCase);
            query = Regex.Replace(query, createView, alterView, RegexOptions.IgnoreCase);

            return query;
        }

        private static string RetornaMetodoObjeto(string[] linhas)
        {
            var createProcedure = "create procedure ";
            var alterProcedure = "alter procedure ";

            var createFunction = "create function ";
            var alterFunction = "alter function ";

            var createView = "create view ";
            var alterView = "alter view ";

            string metodo = "";
            metodo = (linhas.Any(x => x.ToLower().Contains(createProcedure)) ? createProcedure : metodo);
            metodo = (linhas.Any(x => x.ToLower().Contains(alterProcedure)) ? alterProcedure : metodo);

            metodo = (linhas.Any(x => x.ToLower().Contains(createFunction)) ? createFunction : metodo);
            metodo = (linhas.Any(x => x.ToLower().Contains(alterFunction)) ? alterFunction : metodo);

            metodo = (linhas.Any(x => x.ToLower().Contains(createView)) ? createView : metodo);
            metodo = (linhas.Any(x => x.ToLower().Contains(alterView)) ? alterView : metodo);

            return metodo;
        }
    }
}
