using Newtonsoft.Json;
using Semiodesk.Trinity.Configuration;
using System;
using System.Collections.Generic;
using System.Text;
using VDS.RDF;

namespace Semiodesk.Trinity.Store.GraphDB
{
    public class GraphDbModel : Model, IGraphDbModel
    {

        private IGraphDbStore _store;

        public GraphDbModel(IGraphDbStore store, UriRef uri) : base(store, uri)
        {
            _store = store;
        }

        public bool IsEmpty(IGraphDbTransaction transaction)
        {
            SparqlQuery query =
                new SparqlQuery(string.Format(@"ASK FROM {0} {{ ?s ?p ?o . }}", SparqlSerializer.SerializeUri(Uri)));
            return !ExecuteQuery(query, false, transaction).GetAnwser();
        }

        /// <summary>
        /// Removes all elements from the model.
        /// </summary>
        public void Clear(IGraphDbTransaction transaction)
        {
            if (_store != null)
            {
                _store.RemoveModel(this, transaction);
            }
        }
    }
}
