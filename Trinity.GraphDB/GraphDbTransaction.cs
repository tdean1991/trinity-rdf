using System;
using System.Collections.Generic;
using System.Data;
using System.Text;
using AngleSharp;
using VDS.RDF;

namespace Semiodesk.Trinity.Store.GraphDB
{
    public class GraphDbTransaction : IGraphDbTransaction
    {
        private readonly GraphDBConnector _connector;
        public GraphDbTransaction(Guid transactionId, GraphDBConnector connector)
        {
            TransactionId = transactionId;
            _connector = connector;
        }
        public IsolationLevel IsolationLevel => throw new NotImplementedException();

        public Guid TransactionId { get; }

        public event FinishedTransactionEvent OnFinishedTransaction;

        public void Commit()
        {
            _connector.Commit(this);
        }

        public void Dispose()
        {
        }

        public void Rollback()
        {
            _connector.Rollback(this);
        }
    }
}
