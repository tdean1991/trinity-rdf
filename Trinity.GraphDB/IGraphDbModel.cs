using Semiodesk.Trinity.Configuration;
using System;
using System.Collections.Generic;
using System.Text;

namespace Semiodesk.Trinity.Store.GraphDB
{
    public interface IGraphDbModel : IModel
    {
        bool IsEmpty(IGraphDbTransaction transaction);

        /// <summary>
        /// Removes all elements from the model.
        /// </summary>
        void Clear(IGraphDbTransaction transaction);
    }
}
