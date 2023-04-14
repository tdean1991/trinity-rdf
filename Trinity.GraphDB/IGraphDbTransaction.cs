using System;
using System.Collections.Generic;
using System.Text;

namespace Semiodesk.Trinity.Store.GraphDB
{
    public interface IGraphDbTransaction : ITransaction
    {
        Guid TransactionId { get; }
    }
}
