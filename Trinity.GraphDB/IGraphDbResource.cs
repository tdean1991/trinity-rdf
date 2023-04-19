using System;
using System.Collections.Generic;
using System.Text;

namespace Semiodesk.Trinity.Store.GraphDB
{
    public interface IGraphDbResource : IResource
    {
        /// <summary>
        /// Applies the model changes to the transaction
        /// </summary>
        /// <param name="transaction"></param>
        void Persist(IGraphDbTransaction transaction);

        /// <summary>
        /// Reloads from the state from the transaction.
        /// </summary>
        /// <param name="transaction"></param>
        void Reload(IGraphDbTransaction transaction);

    }
}
