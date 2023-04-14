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

       

        #endregion

        #region Methods


        [OneTimeSetUp]
        public void OneTimeSetup()
        {
            var environment = (IStoreTestSetup)Activator.CreateInstance(typeof(GraphDBTestSetup));

            BaseUri = environment.BaseUri;
            ConnectionString = environment.ConnectionString;

            environment.LoadProvider();

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
            Store.Begin();
            var moeUri = BaseUri.GetUriRef("moe");
            var moe = Model1.CreateResource(moeUri);
            moe.AddProperty(rdf.type, nco.PersonContact);
            moe.AddProperty(nco.fullname, "Moe Howard");
            moe.AddProperty(nco.birthDate, DateTime.Now);
            moe.AddProperty(nco.blogUrl, "http://blog.com/moe");
            moe.Commit();
            Store.Commit();
            var moe2 = Model1.GetResource(moeUri);
            Assert.AreEqual(moe.GetValue(nco.fullname), moe2.GetValue(nco.fullname));
        }

        [Test]
        public void CanCreateResourceAndRollback()
        {
            InitializeModels();
            Store.Begin();
            var larryUri = BaseUri.GetUriRef("larry");
            var larry = Model1.CreateResource(larryUri);
            larry.AddProperty(rdf.type, nco.PersonContact);
            larry.AddProperty(nco.fullname, "Larry Fine");
            larry.AddProperty(nco.birthDate, DateTime.Now);
            larry.AddProperty(nco.blogUrl, "http://blog.com/larry");
            larry.Commit();

            var larry2 = Model1.GetResource(larryUri);
            Assert.AreEqual(larry.GetValue(nco.fullname), larry2.GetValue(nco.fullname));
            Store.Rollback();
            Assert.Throws<ResourceNotFoundException>(() => Model1.GetResource(larryUri));
        }

        [Test]
        public void CanDeleteResourceAndRollback()
        {
            InitializeModels();
            var curlyUri = BaseUri.GetUriRef("curly");
            var curly = Model1.GetResource(curlyUri);

            Store.Begin();
            Model1.DeleteResource(curly);
            Assert.Throws<ResourceNotFoundException>(() => Model1.GetResource(curlyUri));
            Store.Rollback();
            var moe2 = Model1.GetResource(curlyUri);
            Assert.AreEqual(curly.GetValue(nco.fullname), moe2.GetValue(nco.fullname));
            
        }

        [Test]
        public void CanDeleteResourceAndCommit()
        {
            InitializeModels();
            var curlyUri = BaseUri.GetUriRef("curly");
            var curly = Model1.GetResource(curlyUri);

            Store.Begin();
            Model1.DeleteResource(curly);
            Store.Commit();
            Assert.Throws<ResourceNotFoundException>(() => Model1.GetResource(curlyUri));
        }

        [Test]
        public void CanUpdateResourceAndCommit()
        {
            InitializeModels();
            var curlyUri = BaseUri.GetUriRef("curly");
            var curly = Model1.GetResource(curlyUri);

            Store.Begin();
            curly.AddProperty(nco.fullname, "Moe Howard");
            curly.Commit();
            Store.Commit();
            var curly2 = Model1.GetResource(curlyUri);
            Assert.AreEqual("Moe Howard", curly2.GetValue(nco.fullname));
            
        }

        [Test]
        public void CanUpdateResourceAndRollback()
        {
            InitializeModels();
            var curlyUri = BaseUri.GetUriRef("curly");
            var curly = Model1.GetResource(curlyUri);

            Store.Begin();
            curly.AddProperty(nco.fullname, "Moe Howard");
            curly.Commit();
            var curly2 = Model1.GetResource(curlyUri);
            Assert.AreEqual("Moe Howard", curly2.GetValue(nco.fullname));
            Store.Rollback();
            var curly3 = Model1.GetResource(curlyUri);
            Assert.AreEqual("Curly Howard", curly3.GetValue(nco.fullname));

        }




        #endregion
    }
}
