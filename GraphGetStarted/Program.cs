namespace GraphGetStarted
{
    using System;
    using System.Collections.Generic;
    using System.Configuration;
    using System.IO;
    using System.Linq;
    using System.Security;
    using System.Text;
    using System.Threading.Tasks;
    using Microsoft.Azure.Documents;
    using Microsoft.Azure.Documents.Client;
    using Microsoft.Azure.Documents.Linq;
    using Microsoft.Azure.Graphs;
    using Microsoft.Azure.Graphs.Elements;
    using Newtonsoft.Json;

    /// <summary>
    /// Sample program that shows how to get started with the Graph (Gremlin) APIs for Azure Cosmos DB.
    /// </summary>
    public class Program
    {
        /// <summary>
        /// Runs some Gremlin commands on the console.
        /// </summary>
        /// <param name="args">command-line arguments</param>
        public static void Main(string[] args)
        {
            string endpoint = ConfigurationManager.AppSettings["Endpoint"];
            SecureString authKey = new SecureString();
               ConfigurationManager.AppSettings["AuthKey"].ToCharArray().ToList().ForEach(c => authKey.AppendChar(c));

            using (DocumentClient client = new DocumentClient(
                new Uri(endpoint),
                authKey,
                new ConnectionPolicy { ConnectionMode = ConnectionMode.Direct, ConnectionProtocol = Protocol.Tcp }))
            {
                Program p = new Program();
                p.RunAsync(client).Wait();
            }
        }

        /// <summary>
        /// Run the get started application.
        /// </summary>
        /// <param name="client">The DocumentDB client instance</param>
        /// <returns>A Task for asynchronous execuion.</returns>
        public async Task RunAsync(DocumentClient client)
        {
            Database database = await client.CreateDatabaseIfNotExistsAsync(new Database { Id = "anvesa" });

            DocumentCollection graph = await client.CreateDocumentCollectionIfNotExistsAsync(
                UriFactory.CreateDatabaseUri("anvesa"),
                new DocumentCollection { Id = "emails-ii" },
                new RequestOptions { OfferThroughput = 1000 });

            // Azure Cosmos DB supports the Gremlin API for working with Graphs. Gremlin is a functional programming language composed of steps.
            // Here, we run a series of Gremlin queries to show how you can add vertices, edges, modify properties, perform queries and traversals
            // For additional details, see https://aka.ms/gremlin for the complete list of supported Gremlin operators
            Dictionary<string, string> gremlinQueries = new Dictionary<string, string>
            {
                //{ "Cleanup",        "g.V().drop()" },

            };

            await QueryGremlin(client, graph, new KeyValuePair<string, string>("Cleanup", "g.V().drop()"));

            var read = new EmailData().GetCSV();
            
            for (int i = 0; i < 10; i++)
            {

                try
                {
                    var email = read[i];
                    string to = email.DocTo.Replace("\"", "");
                    string from = email.DocFrom.Replace("\"", "");
                    string subject = email.EmailSubject;
                    string begDoc = email.BegDoc;

                    //DocTo
                    var result = await QueryGremlin(client, graph, new KeyValuePair<string, string>($"{to}", CheckVerticesExistanceQuery("person", to)));
                    //var resultBool = await CreateNetQuery(client, graph, $"g.V().has('name','{to}')");
                    if (string.IsNullOrEmpty(result))
                    {
                        //gremlinQueries.Add($"AddVertexFor{line[6]}", $"g.AddV('person').property('name','{line[6]}')");
                        await QueryGremlin(client, graph, new KeyValuePair<string, string>($"AddVertexFor{to}", AddVertices("name", to, "person")));
                    }

                    //DocFrom
                    result = await QueryGremlin(client, graph, new KeyValuePair<string, string>($"{from}", CheckVerticesExistanceQuery("name", from)));
                    if (string.IsNullOrEmpty(result))
                    {
                        //gremlinQueries.Add($"AddVertexFor{line[7]}", $"g.AddV('person').property('name','{line[7]}')");
                        await QueryGremlin(client, graph, new KeyValuePair<string, string>($"AddVertexFor{from}", AddVertices("name", from, "person")));
                    }

                    //result = await QueryGremlin(client, graph, new KeyValuePair<string, string>($"{line[8]}", $"g.V('{line[8]}')"));
                    //if (string.IsNullOrEmpty(result))
                    //{
                    //    gremlinQueries.Add($"AddVertexFor{line[8]}", $"g.AddV('email').property('subject','{line[28]}').property('BegDoc','{line[8]}')");
                    //}

                    result = await QueryGremlin(client, graph, new KeyValuePair<string, string>($"{begDoc}", CheckVerticesExistanceQuery("subject",subject)));
                    if (string.IsNullOrEmpty(result))
                    {
                        //gremlinQueries.Add($"AddVertexFor{line[12]}", $"g.AddV('email').property('subject','{line[28]}')");
                        await QueryGremlin(client, graph, new KeyValuePair<string, string>($"AddVertexFor{begDoc}", AddVertices("subject",subject, "email")));
                    }

                    await QueryGremlin(client, graph, new KeyValuePair<string, string>($"AddEdge1", $"g.V().has('name','{to}').addE('sent').to(g.V().has('subject','{subject}'))"));
                    await QueryGremlin(client, graph, new KeyValuePair<string, string>($"AddEdge2", $"g.V().has('name','{from}').addE('received').to(g.V().has('subject','{subject}'))"));
                }
                catch (Exception ex)
                {

                }

            }

            //    { "AddVertex 1",    "g.addV('person').property('id', 'thomas').property('firstName', 'Thomas').property('age', 44)" },
            //    { "AddVertex 2",    "g.addV('person').property('id', 'mary').property('firstName', 'Mary').property('lastName', 'Andersen').property('age', 39)" },
            //    { "AddVertex 3",    "g.addV('person').property('id', 'ben').property('firstName', 'Ben').property('lastName', 'Miller')" },
            //    { "AddVertex 4",    "g.addV('person').property('id', 'robin').property('firstName', 'Robin').property('lastName', 'Wakefield')" },
            //    { "AddEdge 1",      "g.V('thomas').addE('knows').to(g.V('mary'))" },
            //    { "AddEdge 2",      "g.V('thomas').addE('knows').to(g.V('ben'))" },
            //    { "AddEdge 3",      "g.V('ben').addE('knows').to(g.V('robin'))" },
            //    { "UpdateVertex",   "g.V('thomas').property('age', 44)" },
            //    { "CountVertices",  "g.V().count()" },
            //    { "Filter Range",   "g.V().hasLabel('person').has('age', gt(40))" },
            //    { "Project",        "g.V().hasLabel('person').values('firstName')" },
            //    { "Sort",           "g.V().hasLabel('person').order().by('firstName', decr)" },
            //    { "Traverse",       "g.V('thomas').outE('knows').inV().hasLabel('person')" },
            //    { "Traverse 2x",    "g.V('thomas').outE('knows').inV().hasLabel('person').outE('knows').inV().hasLabel('person')" },
            //    { "Loop",           "g.V('thomas').repeat(out()).until(has('id', 'robin')).path()" },
            //    { "DropEdge",       "g.V('thomas').outE('knows').where(inV().has('id', 'mary')).drop()" },
            //    { "CountEdges",     "g.E().count()" },
            //    { "DropVertex",     "g.V('thomas').drop()" },
            //};

            foreach (KeyValuePair<string, string> gremlinQuery in gremlinQueries)
            {
                Console.WriteLine($"Running {gremlinQuery.Key}: {gremlinQuery.Value}");

                //await QueryGremlin(client, graph, gremlinQuery);

                Console.WriteLine();
            }

            // Data is returned in GraphSON format, which be deserialized into a strongly-typed vertex, edge or property class
            // The following snippet shows how to do this
            string gremlin = gremlinQueries["AddVertex 1"];
            Console.WriteLine($"Running Add Vertex with deserialization: {gremlin}");

            //await CreateNetQuery(client, graph, );

            Console.WriteLine();

            Console.WriteLine("Done. Press any key to exit...");
            Console.ReadLine();
        }

        string CheckVerticesExistanceQuery(string propName, string variable)
        {
            return $"g.V().has('{propName}','{variable}')";
        }

        string AddVertices(string propName, string variable, string label)
        {
            return $"g.AddV('{label}').property('{propName}','{variable}')";
        }

        private static async Task<bool> CreateNetQuery(DocumentClient client, DocumentCollection graph, string query)
        {
            IDocumentQuery<Vertex> insertVertex = client.CreateGremlinQuery<Vertex>(graph, query);
            while (insertVertex.HasMoreResults)
            {
                foreach (Vertex vertex in await insertVertex.ExecuteNextAsync<Vertex>())
                {
                    // Since Gremlin is designed for multi-valued properties, the format returns an array. Here we just read
                    // the first value
                    string name = (string)vertex.GetVertexProperties("name").First().Value;
                    Console.WriteLine($"\t Id:{vertex.Id}, Name: {name}");
                }
            }
            return true;
        }

        private static async Task<string> QueryGremlin(DocumentClient client, DocumentCollection graph, KeyValuePair<string, string> gremlinQuery)
        {
            StringBuilder rtn = new StringBuilder() ;
            // The CreateGremlinQuery method extensions allow you to execute Gremlin queries and iterate
            // results asychronously
            IDocumentQuery<dynamic> query = client.CreateGremlinQuery<dynamic>(graph, gremlinQuery.Value);
            while (query.HasMoreResults)
            {
                foreach (dynamic result in await query.ExecuteNextAsync())
                {
                    var r = JsonConvert.SerializeObject(result);
                    Console.WriteLine($"\t {r}");
                    rtn.AppendLine(r);
                }
            }

            return rtn.ToString();
        }


    }
}
