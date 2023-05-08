// LICENSE:
//
// Permission is hereby granted, free of charge, to any person obtaining a copy
// of this software and associated documentation files (the "Software"), to deal
// in the Software without restriction, including without limitation the rights
// to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
// copies of the Software, and to permit persons to whom the Software is
// furnished to do so, subject to the following conditions:
//
// The above copyright notice and this permission notice shall be included in
// all copies or substantial portions of the Software.
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
// IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
// AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
// LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
// OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
// THE SOFTWARE.
//
// AUTHORS:
//
//  Moritz Eberl <moritz@semiodesk.com>
//  Sebastian Faubel <sebastian@semiodesk.com>
//
// Copyright (c) Semiodesk GmbH 2023

using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Net;
using System.Text.RegularExpressions;
using System.Text;
using System.Web;
using System;
using System.Data.SqlTypes;
using System.Transactions;
using System.Xml.Schema;
using System.Xml.Xsl;
using VDS.RDF.Parsing.Handlers;
using VDS.RDF.Parsing;
using VDS.RDF.Query;
using VDS.RDF.Storage;
using VDS.RDF;
using IsolationLevel = System.Data.IsolationLevel;
using VDS.RDF.Writing.Formatting;
using System.Collections;

namespace Semiodesk.Trinity.Store.GraphDB
{
    /// <summary>
    /// Class for connecting to GraphDB triple stores.
    /// </summary>
    public class GraphDBConnector : SesameHttpProtocolVersion6Connector
    {
        protected readonly string _transactionsPath = "/transactions";
        private NTriplesFormatter _formatter = new NTriplesFormatter();
        #region Constructors

        /// <summary>
        /// Creates a new connection to a Sesame HTTP Protocol supporting Store.
        /// </summary>
        /// <param name="baseUri">URL of the database server.</param>
        /// <param name="repositoryName">Name of the GraphDB repository.</param>
        /// <param name="username">Username to use for requests that require authentication.</param>
        /// <param name="password">Password to use for requests that require authentication.</param>
        public GraphDBConnector(string baseUri, string repositoryName, string username, string password)
            : base(baseUri, repositoryName, username, password)
        {
        }

        #endregion

        #region Methods

        /// <summary>
        /// Makes a SPARQL Query against the underlying Store.
        /// </summary>
        /// <param name="sparqlQuery">SPARQL Query.</param>
        /// <param name="allowPlainTextResults">Indicate if the query may return results that have no RDF metadata.</param>
        /// <param name="inferenceEnabled">Indicate if the query should be executed with reasoning.</param>
        /// <returns></returns>
        public object Query(string sparqlQuery, bool allowPlainTextResults, bool inferenceEnabled, IGraphDbTransaction transaction)
        {
            var graph = new Graph();
            var results = new SparqlResultSet();

            Query(new GraphHandler(graph), new ResultSetHandler(results), sparqlQuery, allowPlainTextResults,
                inferenceEnabled, transaction);

            return results.ResultsType != SparqlResultsType.Unknown ? (object)results : (object)graph;
        }

        /// <summary>
        /// Makes a SPARQL Query against the underlying Store processing the results with an appropriate handler from those provided.
        /// </summary>
        /// <param name="rdfHandler">RDF Handler.</param>
        /// <param name="resultsHandler">Results Handler.</param>
        /// <param name="sparqlQuery">SPARQL Query.</param>
        /// <returns></returns>
        public override void Query(IRdfHandler rdfHandler, ISparqlResultsHandler resultsHandler, string sparqlQuery)
        {
            Query(rdfHandler, resultsHandler, sparqlQuery, true, false, null);
        }

        /// <summary>
        /// Makes a SPARQL Query against the underlying Store processing the results with an appropriate handler from those provided.
        /// </summary>
        /// <param name="rdfHandler">RDF Handler.</param>
        /// <param name="resultsHandler">Results Handler.</param>
        /// <param name="sparqlQuery">SPARQL Query.</param>
        /// <param name="transaction">GraphDb Transaction</param>
        /// <returns></returns>
        public void Query(IRdfHandler rdfHandler, ISparqlResultsHandler resultsHandler, string sparqlQuery, IGraphDbTransaction transaction)
        {
            Query(rdfHandler, resultsHandler, sparqlQuery, true, false, transaction);
        }

        /// <summary>
        /// Makes a SPARQL Query against the underlying Store processing the results with an appropriate handler from those provided.
        /// </summary>
        /// <param name="rdfHandler">RDF Handler.</param>
        /// <param name="resultsHandler">Results Handler.</param>
        /// <param name="sparqlQuery">SPARQL Query.</param>
        /// <param name="allowPlainTextResults">Indicate if the query may return results that have no RDF metadata.</param>
        /// <param name="inferenceEnabled">Indicate if the query should be executed with reasoning.</param>
        /// /// <param name="transaction">GraphDb Transaction</param>
        /// <returns></returns>
        public virtual void Query(IRdfHandler rdfHandler, ISparqlResultsHandler resultsHandler, string sparqlQuery,
            bool allowPlainTextResults, bool inferenceEnabled, IGraphDbTransaction transaction)
        {
            try
            {
                VDS.RDF.Query.SparqlQuery sparqlQuery1 = null;

                string accept;

                if (allowPlainTextResults)
                {
                    try
                    {
                        sparqlQuery1 = new SparqlQueryParser().ParseFromString(sparqlQuery);
                        allowPlainTextResults = sparqlQuery1.QueryType == VDS.RDF.Query.SparqlQueryType.Ask;
                    }
                    catch
                    {
                        allowPlainTextResults = Regex.IsMatch(sparqlQuery, "ASK", RegexOptions.IgnoreCase);
                    }

                    accept = sparqlQuery1 == null
                        ? MimeTypesHelper.HttpRdfOrSparqlAcceptHeader
                        : (SparqlSpecsHelper.IsSelectQuery(sparqlQuery1.QueryType) ||
                           sparqlQuery1.QueryType == VDS.RDF.Query.SparqlQueryType.Ask
                            ? MimeTypesHelper.HttpSparqlAcceptHeader
                            : MimeTypesHelper.HttpAcceptHeader);
                }
                else
                {
                    accept = MimeTypesHelper.HttpAcceptHeader;
                }

                var url = "";
                var method = "";
                HttpWebRequest request;
                
                var parameters = new Dictionary<string, string>();
                if (inferenceEnabled)
                {
                    parameters["infer"] = "true";
                }

                if (transaction is null)
                {
                    url = $"{_repositoriesPrefix}{_store}{_queryPath}";
                    method = "POST";
                    request = CreateRequest(url, accept, method, parameters);
                    request.ContentType = "application/x-www-form-urlencoded;charset=utf-8";
                    var queryBuilder = new StringBuilder();
                    queryBuilder.Append("query=");
                    queryBuilder.Append(HttpUtility.UrlEncode(this.EscapeQuery(sparqlQuery)));

                    using (var writer =
                           new StreamWriter(request.GetRequestStream(), new UTF8Encoding(Options.UseBomForUtf8)))
                    {
                        writer.Write(queryBuilder);
                        writer.Close();
                    }
                }
                else
                {
                    url = GetTransactionUri(transaction);
                    method = "PUT";
                    parameters.Add("action", "QUERY");
                    parameters.Add("query", this.EscapeQuery(sparqlQuery));
                    request = CreateRequest(url, accept, method, parameters);
                    request.ContentType = "application/x-www-form-urlencoded;charset=utf-8";
                }

                


                Tools.HttpDebugRequest(request);

                using (HttpWebResponse response = (HttpWebResponse)request.GetResponse())
                {
                    Tools.HttpDebugResponse(response);

                    var input = new StreamReader(response.GetResponseStream());

                    try
                    {
                        MimeTypesHelper.GetSparqlParser(response.ContentType, allowPlainTextResults)
                            .Load(resultsHandler, input);
                        response.Close();
                    }
                    catch (RdfParserSelectionException)
                    {
                        if (response.ContentType.StartsWith("application/xml"))
                        {
                            try
                            {
                                MimeTypesHelper.GetSparqlParser("application/sparql-results+xml")
                                    .Load(resultsHandler, input);
                                response.Close();
                            }
                            catch (RdfParserSelectionException)
                            {
                            }
                        }

                        var parser = MimeTypesHelper.GetParser(response.ContentType);

                        if (sparqlQuery1 != null && (SparqlSpecsHelper.IsSelectQuery(sparqlQuery1.QueryType) ||
                                                     sparqlQuery1.QueryType == VDS.RDF.Query.SparqlQueryType.Ask))
                            new SparqlRdfParser(parser).Load(resultsHandler, input);
                        else
                        {
                            parser.Load(rdfHandler, input);
                        }

                        response.Close();
                    }
                }
            }
            catch (WebException ex)
            {
                throw StorageHelper.HandleHttpQueryError(ex);
            }
        }

        // Summary:
        //     Makes a SPARQL Query against the underlying Store.
        //
        // Parameters:
        //   sparqlQuery:
        //     SPARQL Query.
        public object Query(string sparqlQuery, IGraphDbTransaction transaction)
        {
            Graph graph = new Graph();
            SparqlResultSet sparqlResultSet = new SparqlResultSet();
            Query(new GraphHandler(graph), new ResultSetHandler(sparqlResultSet), sparqlQuery, transaction);
            if (sparqlResultSet.ResultsType != SparqlResultsType.Unknown)
            {
                return sparqlResultSet;
            }

            return graph;
        }


        public override IEnumerable<Uri> ListGraphs() => ListGraphs(null);


        public IEnumerable<Uri> ListGraphs(IGraphDbTransaction transaction)
        {
            try
            {
                // Note: This query fails if allowPlainTextResults is true which is the default in dotNetRdf.
                var obj = Query("SELECT DISTINCT ?g WHERE { GRAPH ?g { ?s ?p ?o } }", false, false,  transaction);

                if (!(obj is SparqlResultSet))
                {
                    return Enumerable.Empty<Uri>();
                }

                var uriList = new List<Uri>();

                foreach (SparqlResult sparqlResult in (SparqlResultSet)obj)
                {
                    if (sparqlResult.HasValue("g"))
                    {
                        INode node = sparqlResult["g"];

                        if (node.NodeType == NodeType.Uri)
                        {
                            uriList.Add(((IUriNode)node).Uri);
                        }
                    }
                }

                return uriList;
            }
            catch (Exception ex)
            {
                throw StorageHelper.HandleError(ex, "listing Graphs from");
            }
        }

        public IGraphDbTransaction BeginTransaction(System.Data.IsolationLevel isolationLevel = IsolationLevel.Unspecified)
        {

            var reqUri = StartTransactionUri;
            var request = this.CreateRequest(reqUri, "*/*", WebRequestMethods.Http.Post, new Dictionary<string, string>());
            Tools.HttpDebugRequest(request);
            using (HttpWebResponse response = (HttpWebResponse)request.GetResponse())
            {
                Tools.HttpDebugResponse(response);

                var input = new StreamReader(response.GetResponseStream());
                Guid transactionId;
                try
                {
                    var transactionUrl = response.GetResponseHeader("Location");
                    var regex = new Regex($"^.*{_repositoriesPrefix}{_store}{_transactionsPath}/(?<transactionId>.*)");
                    var match = regex.Match(transactionUrl);
                    transactionId = new Guid(match.Groups["transactionId"].ToString());
                    response.Close();
                }
                catch (WebException webEx)
                {
                    throw StorageHelper.HandleHttpError(webEx, "BeginTransaction");
                }
                catch (Exception ex)
                {
                    throw StorageHelper.HandleError(ex, "BeginTransaction");
                }


                return new GraphDbTransaction(transactionId, this);
            }
        }

        public void Rollback(IGraphDbTransaction transaction)
        {
            var reqUri = GetTransactionUri(transaction);

            var request = this.CreateRequest(reqUri, "*/*", "PUT", new Dictionary<string, string>());
            Tools.HttpDebugRequest(request);
            try
            {
                HttpWebResponse response = (HttpWebResponse)request.GetResponse();
            }
            catch (WebException webEx)
            {
                throw StorageHelper.HandleHttpError(webEx, "RollbackTransaction");
            }
            catch (Exception ex)
            {
                throw StorageHelper.HandleError(ex, "RollbackTransaction");
            }

        }

        public void Commit(IGraphDbTransaction transaction)
        {
            var reqUri = GetTransactionUri(transaction);
            var queryParams = new Dictionary<string, string>()
            {
                {"action", "COMMIT"},
            };
            var request = this.CreateRequest(reqUri, "application/rdf+xml", "PUT", queryParams);
            Tools.HttpDebugRequest(request);
            try
            {
                HttpWebResponse response = (HttpWebResponse)request.GetResponse();
            }
            catch (WebException webEx)
            {
                throw StorageHelper.HandleHttpError(webEx, "CommitTransaction");
            }
            catch (Exception ex)
            {
                throw StorageHelper.HandleError(ex, "CommitTransaction");
            }

        }

        public void Update(Uri graphUri, string sparqlUpdate, IGraphDbTransaction transaction) => Update(sparqlUpdate, transaction, ToSafeString(graphUri));

        public void Update(string sparqlUpdate, IGraphDbTransaction transaction) => Update(sparqlUpdate, transaction, null);


        /// <summary>Makes a SPARQL Update request to the Sesame server.</summary>
        /// <param name="sparqlUpdate">SPARQL Update.</param>
        private void Update(string sparqlUpdate, IGraphDbTransaction transaction, string baseUri)
        {            
            try
            {
                HttpWebRequest request;
                var uri = "";
                var method = "";
                var queryParams = new Dictionary<string, string>();
                uri = GetTransactionUri(transaction);
                method = "PUT";
                queryParams.Add("action", "UPDATE");
                if (!(baseUri is null))
                {
                    queryParams.Add("baseUri", baseUri);
                }
                queryParams.Add("update", EscapeQuery(sparqlUpdate));
                request = this.CreateRequest(uri,"*/*", method, queryParams);
                request.ContentType = "application/x-www-form-urlencoded;charset=utf-8";
                
                Tools.HttpDebugRequest(request);
                using (HttpWebResponse response = (HttpWebResponse)request.GetResponse())
                {
                    Tools.HttpDebugResponse(response);
                    response.Close();
                }
            }
            catch (WebException ex)
            {
                throw StorageHelper.HandleHttpError(ex, "updating");
            }
        }
        #endregion

        //
        // Summary:
        //     Updates a Graph.
        //
        // Parameters:
        //   graphUri:
        //     Uri of the Graph to update.
        //
        //   additions:
        //     Triples to be added.
        //
        //   removals:
        //     Triples to be removed.
        public void UpdateGraph(Uri graphUri, IEnumerable<Triple> additions, IEnumerable<Triple> removals, IGraphDbTransaction transaction)
        {
            UpdateGraph(ToSafeString(graphUri), additions, removals, transaction);
        }

        //
        // Summary:
        //     Updates a Graph.
        //
        // Parameters:
        //   graphUri:
        //     Uri of the Graph to update.
        //
        //   additions:
        //     Triples to be added.
        //
        //   removals:
        //     Triples to be removed.
        public void UpdateGraph(string graphUri, IEnumerable<Triple> additions, IEnumerable<Triple> removals, IGraphDbTransaction transaction)
        {
            try
            {
                Dictionary<string, string> dictionary = new Dictionary<string, string>();
                IRdfWriter rdfWriter = CreateRdfWriter();
                dictionary.Add("baseUri", graphUri);

                if (removals != null && removals.Any())
                {
                    foreach (Triple item in removals.Distinct())
                    {
                        dictionary["action"] = "UPDATE";


                        var tstr = _formatter.Format(item);
                        var query = $@"DELETE DATA {{ GRAPH <{graphUri}> {{{tstr}}} }} ";
                        dictionary["update"] = query;
                        HttpWebRequest httpWebRequest = CreateRequest(GetTransactionUri(transaction), "*/*", "PUT", dictionary);
                        Tools.HttpDebugRequest(httpWebRequest);
                        HttpWebResponse httpWebResponse;
                        using (httpWebResponse = (HttpWebResponse)httpWebRequest.GetResponse())
                        {
                            Tools.HttpDebugResponse(httpWebResponse);
                            httpWebResponse.Close();
                        }
                    }

                    
                }
                
                if (additions != null && additions.Any())
                {
                    foreach (Triple item in additions.Distinct())
                    {
                        dictionary["action"] = "UPDATE";
                        var tstr = _formatter.Format(item);
                        dictionary["update"] = $@"INSERT DATA {{  GRAPH <{graphUri}> {{{tstr}}} }}";
                        HttpWebRequest httpWebRequest = CreateRequest(GetTransactionUri(transaction), "*/*", "PUT", dictionary);
                        httpWebRequest.ContentType = GetSaveContentType();
                        Tools.HttpDebugRequest(httpWebRequest);
                        HttpWebResponse httpWebResponse;
                        using (httpWebResponse = (HttpWebResponse)httpWebRequest.GetResponse())
                        {
                            Tools.HttpDebugResponse(httpWebResponse);
                            httpWebResponse.Close();
                        }
                    }
                }
            }
            catch (WebException webEx)
            {
                throw StorageHelper.HandleHttpError(webEx, "updating a Graph in");
            }
        }

        /// <summary>Deletes a Graph from the Sesame store.</summary>
        /// <param name="graphUri">URI of the Graph to delete.</param>
        public void DeleteGraph(Uri graphUri, IGraphDbTransaction transaction) => this.DeleteGraph(ToSafeString(graphUri), transaction);

        /// <summary>Deletes a Graph from the Sesame store.</summary>
        /// <param name="graphUri">URI of the Graph to delete.</param>
        public void DeleteGraph(string graphUri, IGraphDbTransaction transaction)
        {
            try
            {
                Dictionary<string, string> queryParams = new Dictionary<string, string>();
                queryParams.Add("action", "DELETE");
                queryParams.Add("baseURI", graphUri);
                HttpWebRequest request = this.CreateRequest(this.GetTransactionUri(transaction), "*/*", "PUT", queryParams);
                Tools.HttpDebugRequest(request);
                HttpWebResponse response;
                using (response = (HttpWebResponse)request.GetResponse())
                {
                    Tools.HttpDebugResponse(response);
                    response.Close();
                }
            }
            catch (WebException ex)
            {
                throw StorageHelper.HandleHttpError(ex, "deleting a Graph from");
            }
        }

        /// <summary>
        /// Saves a Graph into the Store (Warning: Completely replaces any existing Graph with the same URI unless there is no URI - see remarks for details).
        /// </summary>
        /// <param name="g">Graph to save.</param>
        /// <remarks>
        /// If the Graph has no URI then the contents will be appended to the Store, if the Graph has a URI then existing data associated with that URI will be replaced.
        /// </remarks>
        public void SaveGraph(IGraph g, IGraphDbTransaction transaction)
        {
            try
            {
                Dictionary<string, string> queryParams = new Dictionary<string, string>();
                queryParams.Add("action", "ADD");
                HttpWebRequest request;
                //if (g.BaseUri != (Uri)null)
                //{
                    //if (this._fullContextEncoding)
                    //    queryParams.Add("context", "<" + g.BaseUri.AbsoluteUri + ">");
                    //else
                queryParams.Add("baseUri", g.BaseUri.AbsoluteUri);
                request = this.CreateRequest(this.GetTransactionUri(transaction), "*/*", "PUT", queryParams);

                //}
                //else
                //    request = this.CreateRequest(this._repositoriesPrefix + this._store + "/statements", "*/*", "POST", queryParams);
                request.ContentType = this.GetSaveContentType();
                this.CreateRdfWriter().Save(g, (TextWriter)new StreamWriter(request.GetRequestStream()));
                Tools.HttpDebugRequest(request);
                using (HttpWebResponse response = (HttpWebResponse)request.GetResponse())
                {
                    Tools.HttpDebugResponse(response);
                    response.Close();
                }
            }
            catch (WebException ex)
            {
                throw StorageHelper.HandleHttpError(ex, "save a Graph to");
            }
        }

        public override void UpdateGraph(string graphUri, IEnumerable<Triple> additions, IEnumerable<Triple> removals)
        {
            base.UpdateGraph(graphUri, additions, removals);
        }

        


        private string StartTransactionUri
        {
            get => $"{_repositoriesPrefix}{this._store}{this._transactionsPath}";
        }

        private string GetTransactionUri(IGraphDbTransaction transaction) => 
            $"{_repositoriesPrefix}{this._store}{this._transactionsPath}/{transaction.TransactionId}";

        private static string ToSafeString(Uri uri) => uri?.ToString() ?? string.Empty;
    }
}