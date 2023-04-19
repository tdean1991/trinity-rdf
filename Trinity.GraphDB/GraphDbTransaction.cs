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
            if(!IsActive)
            {
                throw new InvalidOperationException("Transaction has been previously committed or rolled back.");
            }
            _connector.Commit(this);
            IsActive = false;
        }

        public void Dispose()
        {
        }

        public void Rollback()
        {
            if (!IsActive)
            {
                throw new InvalidOperationException("Transaction has been previously committed or rolled back.");
            }
            _connector.Rollback(this);
            IsActive = false;
        }

        public bool IsActive
        {
            get; private set;
        }
    }
}
