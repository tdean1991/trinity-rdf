using System;
using System.Collections.Generic;
using System.Data;
using System.IO;
using System.Linq;
using System.Reflection;
using System.Runtime.InteropServices;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using System.Web.SessionState;
using NUnit.Framework;
using Semiodesk.Trinity.Ontologies;
using Semiodesk.Trinity.Store.GraphDB;
using Semiodesk.Trinity.Tests.Linq;
using Semiodesk.Trinity.Tests.Store;
using VDS.RDF;
using VDS.RDF.Query;
using VDS.RDF.Storage;

namespace Semiodesk.Trinity.Tests.GraphDB
{
    [TestFixture]
    public class GraphDbTransactionTests
    {

        #region Members

        protected string ConnectionString;

        private Uri _brito;

        private Uri _curly;

        private Uri _curlyPager;

        private Uri _curlyPhone1;

        private Uri _curlyPhone2;

        private UriRef BaseUri;

        protected IGraphDbStore Store;

        protected IModel Model1;

        protected IStoreTestSetup Environment;

        #endregion

        #region Methods


        [OneTimeSetUp]
        public void OneTimeSetup()
        {
            Environment = (IStoreTestSetup)Activator.CreateInstance(typeof(GraphDBTestSetup));

            BaseUri = Environment.BaseUri;
            ConnectionString = Environment.ConnectionString;
            
            Environment.LoadProvider();

            Directory.SetCurrentDirectory(TestContext.CurrentContext.TestDirectory);
            OntologyDiscovery.AddAssembly(Assembly.GetExecutingAssembly());
            MappingDiscovery.RegisterAssembly(Assembly.GetExecutingAssembly());
            OntologyDiscovery.AddAssembly(typeof(AbstractMappingClass).Assembly);
            MappingDiscovery.RegisterAssembly(typeof(AbstractMappingClass).Assembly);

            var location = new FileInfo(Assembly.GetExecutingAssembly().Location);
            var folder = new DirectoryInfo(Path.Combine(location.DirectoryName, "nunit"));

            if (folder.Exists)
            {
                folder.Delete(true);
            }

            folder.Create();

            Store = (GraphDBStore) StoreFactory.CreateStore(ConnectionString);
            Store.InitializeFromConfiguration();

            // Wait until the inference engine has loaded the ontologies..
            Thread.Sleep(1000);
        }

        [SetUp]
        public void SetUp()
        {
            Model1 = Store.GetModel(BaseUri.GetUriRef("model1"));

            if (!Model1.IsEmpty) Model1.Clear();


            OntologyDiscovery.AddNamespace("dbpedia", new Uri("http://dbpedia.org/ontology/"));
            OntologyDiscovery.AddNamespace("dbpprop", new Uri("http://dbpedia.org/property/"));
            OntologyDiscovery.AddNamespace("dc", dc.Namespace);
            OntologyDiscovery.AddNamespace("ex", new Uri("http://example.org/"));
            OntologyDiscovery.AddNamespace("foaf", foaf.Namespace);
            OntologyDiscovery.AddNamespace("nco", nco.Namespace);
            OntologyDiscovery.AddNamespace("nfo", nfo.Namespace);
            OntologyDiscovery.AddNamespace("nie", nie.Namespace);
            OntologyDiscovery.AddNamespace("schema", new Uri("http://schema.org/"));
            OntologyDiscovery.AddNamespace("sfo", sfo.Namespace);
            OntologyDiscovery.AddNamespace("vcard", vcard.Namespace);

            _brito = BaseUri.GetUriRef("brito");
            _curly = BaseUri.GetUriRef("curly");
            _curlyPhone1 = BaseUri.GetUriRef("curlyPhone1");
            _curlyPhone2 = BaseUri.GetUriRef("curlyPhone2");
            _curlyPager = BaseUri.GetUriRef("curlyPager");
        }

        private void InitializeModels()
        {
            var curlyPager = Model1.CreateResource(_curlyPager);
            curlyPager.AddProperty(rdf.type, nco.PagerNumber);
            curlyPager.AddProperty(dc.date, DateTime.Today);
            curlyPager.AddProperty(nco.creator, _curly);
            curlyPager.Commit();

            var curlyPhone1 = Model1.CreateResource(_curlyPhone1);
            curlyPhone1.AddProperty(rdf.type, nco.PhoneNumber);
            curlyPhone1.AddProperty(dc.date, DateTime.Today.AddDays(1));
            curlyPhone1.AddProperty(nco.creator, _curly);
            curlyPhone1.Commit();

            var curlyPhone2 = Model1.CreateResource(_curlyPhone2);
            curlyPhone2.AddProperty(rdf.type, nco.PhoneNumber);
            curlyPhone2.AddProperty(dc.date, DateTime.Today.AddDays(2));
            curlyPhone2.AddProperty(nco.creator, _curly);
            curlyPhone2.Commit();

            var curly = Model1.CreateResource(_curly);
            curly.AddProperty(rdf.type, nco.PersonContact);
            curly.AddProperty(nco.fullname, "Curly Howard");
            curly.AddProperty(nco.birthDate, DateTime.Now);
            curly.AddProperty(nco.blogUrl, "http://blog.com/Curly");
            curly.AddProperty(nco.hasContactMedium, curlyPager);
            curly.AddProperty(nco.hasPhoneNumber, curlyPhone1);
            curly.AddProperty(nco.hasPhoneNumber, curlyPhone2);
            curly.Commit();

            var brito = Model1.CreateResource((_brito));
            brito.AddProperty(rdf.type, nco.OrganizationContact);
            brito.AddProperty(nco.fullname, "BRITO");
            brito.AddProperty(nco.creator, curly);
            brito.Commit();
        }


        [OneTimeTearDown]
        public void OneTimeTearDown()
        {
            Store.Dispose();
        }

        [Test]
        public void CanCreateTransaction()
        {
            var transaction = (IGraphDbTransaction) Store.BeginTransaction(IsolationLevel.Unspecified);
            Assert.IsNotNull(transaction);
            Assert.AreNotEqual(Guid.Empty, transaction.TransactionId);
            transaction.Rollback();
        }

        [Test]
        public void CanRollbackTransaction()
        {
            var transaction = Store.BeginTransaction(IsolationLevel.Unspecified);
            Assert.DoesNotThrow(() => transaction.Rollback());
        }

        [Test]
        public void CanCommitTransaction()
        {
            var transaction = Store.BeginTransaction(IsolationLevel.Unspecified);
            Assert.DoesNotThrow(() => transaction.Commit());
        }

        [Test]
        public void CanQueryWithTransaction()
        {
            InitializeModels(); 
            ITransaction transaction = null;
            transaction = Model1.BeginTransaction(IsolationLevel.Unspecified);
            Assert.IsNotNull(transaction);
            // Retrieving bound variables using the SELECT query form.
            var query = new SparqlQuery("SELECT ?name ?birthday WHERE { ?x nco:fullname ?name. ?x nco:birthDate ?birthday. }");
            var result = Model1.ExecuteQuery(query, false, transaction);

            Assert.AreEqual(1, result.GetBindings().Count());

            // Retrieving resoures using the SELECT or DESCRIBE query form.
            query = new SparqlQuery("SELECT ?s ?p ?o WHERE { ?s ?p ?o. ?s nco:fullname 'Curly Howard'. }");
            result = Model1.ExecuteQuery(query, false, transaction);

            Assert.AreEqual(1, result.GetResources().Count());

            // Test SELECT with custom defined PREFIXes
            query = new SparqlQuery("PREFIX contact: <http://www.semanticdesktop.org/ontologies/2007/03/22/nco#> SELECT ?s ?p ?o WHERE { ?s ?p ?o. ?s contact:fullname 'Curly Howard'. }");
            result = Model1.ExecuteQuery(query, false, transaction);

            Assert.AreEqual(1, result.GetResources().Count());
            transaction.Rollback();
        }

        [Test]
        public void CanCreateResourceAndCommit()
        {
            InitializeModels();
            var transaction = Store.BeginTransaction(IsolationLevel.Unspecified);
            var moeUri = BaseUri.GetUriRef("moe");
            var moe = Model1.CreateResource(moeUri, transaction);
            moe.AddProperty(rdf.type, nco.PersonContact);
            moe.AddProperty(nco.fullname, "Moe Howard");
            moe.AddProperty(nco.birthDate, DateTime.Now);
            moe.AddProperty(nco.blogUrl, "http://blog.com/moe");
            moe.Commit();
            transaction.Commit();
            var moe2 = Model1.GetResource(moeUri);
            Assert.AreEqual(moe.GetValue(nco.fullname), moe2.GetValue(nco.fullname));
        }

        [Test]
        public void CanCreateResourceAndRollback()
        {
            InitializeModels();
            var transaction = Store.BeginTransaction(IsolationLevel.Unspecified);
            var larryUri = BaseUri.GetUriRef("larry");
            var larry = Model1.CreateResource(larryUri, transaction);
            larry.AddProperty(rdf.type, nco.PersonContact);
            larry.AddProperty(nco.fullname, "Larry Fine");
            larry.AddProperty(nco.birthDate, DateTime.Now);
            larry.AddProperty(nco.blogUrl, "http://blog.com/larry");
            larry.Commit();

            var larry2 = Model1.GetResource(larryUri, transaction);
            Assert.AreEqual(larry.GetValue(nco.fullname), larry2.GetValue(nco.fullname));
            transaction.Rollback();
            Assert.Throws<ResourceNotFoundException>(() => Model1.GetResource(larryUri));
        }

        [Test]
        public void CanDeleteResourceAndRollback()
        {
            InitializeModels();
            var curlyUri = BaseUri.GetUriRef("curly");
            var curly = Model1.GetResource(curlyUri);

            using (IGraphDbTransaction transaction = Store.BeginTransaction())
            {
                Model1.DeleteResource(curly, transaction);
                Assert.Throws<ResourceNotFoundException>(() => Model1.GetResource(curlyUri, transaction));
                transaction.Rollback();
            }
            var curly2 = Model1.GetResource(curlyUri);
            Assert.AreEqual(curly.GetValue(nco.fullname), curly2.GetValue(nco.fullname));
            
        }

        [Test]
        public void CanDeleteResourceAndCommit()
        {
            InitializeModels();
            var curlyUri = BaseUri.GetUriRef("curly");
            var curly = Model1.GetResource(curlyUri);

            var transaction = Store.BeginTransaction();
            Model1.DeleteResource(curly, transaction);
            transaction.Commit();
            Assert.Throws<ResourceNotFoundException>(() => Model1.GetResource(curlyUri));
        }

        [Test]
        public void CanUpdateResourceAndCommit()
        {
            InitializeModels();
            var curlyUri = BaseUri.GetUriRef("curly");
            var curly = Model1.GetResource(curlyUri);

            var transaction = Store.BeginTransaction();
            curly.AddProperty(nco.fullname, "Jerome Lester Horwitz");
            curly.Commit();
            transaction.Commit();
            var curly2 = Model1.GetResource(curlyUri);
            Assert.AreEqual("Jerome Lester Horwitz", curly2.GetValue(nco.fullname));
            
        }

        [Test]
        public void CanUpdateResourceAndRollback()
        {
            InitializeModels();
            var curlyUri = BaseUri.GetUriRef("curly");

            using (var transaction = Store.BeginTransaction())
            {
                var curly = Model1.GetResource(curlyUri, transaction);
                curly.AddProperty(nco.fullname, "Jerome Lester Horwitz");
                curly.Commit();
                var curly2 = Model1.GetResource(curlyUri, transaction);
                Assert.AreEqual("Jerome Lester Horwitz", curly2.GetValue(nco.fullname));
                transaction.Rollback();
                var curly3 = Model1.GetResource(curlyUri);
                Assert.AreEqual("Curly Howard", curly3.GetValue(nco.fullname));
            }

        }

        [Test]
        public void CanUpdateTriplesAndCommit()
        {
            InitializeModels();
            var curlyUri = BaseUri.GetUriRef("curly");
            var curly = Model1.GetResource(curlyUri);
            var modelUri = BaseUri.GetUriRef("model1");
            var connector = (GraphDBConnector)typeof(GraphDBStore).GetField("_connector", BindingFlags.Instance | BindingFlags.NonPublic).GetValue(Store);
            var getQuery = $@"SELECT ?s_ ?p_ ?o_ FROM <{modelUri}>
                                WHERE {{ ?s_ ?p_ ?o_ .  FILTER (?s_ = <{curlyUri}>) }}";
            
            var results = (SparqlResultSet)connector.Query(getQuery, false, true, null);
            var nodeFactory = new NodeFactory();
            var triplesToRemove = results.Where(x => x["o_"].ToString() == "Curly Howard")
                .Select(x => new Triple(x["s_"], x["p_"], x["o_"]))
                .ToList();
           
            var triplesToAdd = triplesToRemove.Select(x =>
                new Triple(x.Subject, x.Predicate, nodeFactory.CreateLiteralNode("Jerome Lester Horwitz"))).ToList();
            var transaction = connector.BeginTransaction();
            connector.UpdateGraph(modelUri, triplesToAdd, triplesToRemove, transaction);
            transaction.Commit();

            var afterResults = (SparqlResultSet)connector.Query(getQuery);
            var deletedItems = afterResults.Where(x => x["o_"].ToString() == "Curly Howard").ToList();
            var newResults = afterResults.Where(x => x["o_"].ToString() == "Jerome Lester Horwitz").ToList();
            Assert.AreEqual(0, deletedItems.Count);
            Assert.AreEqual(1, newResults.Count);

        }

        [Test]
        public void CanUpdateTriplesAndRollback()
        {
            InitializeModels();
            var curlyUri = BaseUri.GetUriRef("curly");
            var curly = Model1.GetResource(curlyUri);
            var modelUri = BaseUri.GetUriRef("model1");
            var connector = (GraphDBConnector)typeof(GraphDBStore).GetField("_connector", BindingFlags.Instance | BindingFlags.NonPublic).GetValue(Store);
            var getQuery = $@"SELECT ?s_ ?p_ ?o_ FROM <{modelUri}>
                                WHERE {{ ?s_ ?p_ ?o_ .  FILTER (?s_ = <{curlyUri}>) }}";

            var results = (SparqlResultSet)connector.Query(getQuery, false, true, null);
            var nodeFactory = new NodeFactory();
            var triplesToRemove = results.Where(x => x["o_"].ToString() == "Curly Howard")
                .Select(x => new Triple(x["s_"], x["p_"], x["o_"]))
                .ToList();

            var triplesToAdd = triplesToRemove.Select(x => new Triple(x.Subject, x.Predicate, nodeFactory.CreateLiteralNode("Jerome Lester Horwitz"))).ToList();
            var transaction = connector.BeginTransaction();
            connector.UpdateGraph(modelUri, triplesToAdd, triplesToRemove, transaction);



            var afterResults = (SparqlResultSet)connector.Query(getQuery, transaction);
            var deletedItems = afterResults.Where(x => x["o_"].ToString() == "Curly Howard").ToList();
            var newItems = afterResults.Where(x => x["o_"].ToString() == "Jerome Lester Horwitz").ToList();
            Assert.AreEqual(0, deletedItems.Count);
            Assert.AreEqual(1, newItems.Count);
            transaction.Rollback();

            var rollbackResults = (SparqlResultSet)connector.Query(getQuery);
            var deletedItemsRollback = rollbackResults.Where(x => x["o_"].ToString() == "Curly Howard").ToList();
            var newItemsRollback = rollbackResults.Where(x => x["o_"].ToString() == "Jerome Lester Horwitz").ToList();
            Assert.AreEqual(1, deletedItemsRollback.Count);
            Assert.AreEqual(0, newItemsRollback.Count);
        }




        #endregion
    }
}
