using System;
using System.Collections.Generic;
using System.Text;

namespace Semiodesk.Trinity.Store.GraphDB
{
    public class GraphDbResource : Resource, IGraphDbResource
    {

        /// <summary>
        /// Create a new resource with a given Uri.
        /// </summary>
        /// <param name="uri"></param>
        public GraphDbResource(UriRef uri) : base(uri) { }
        
        /// <summary>
        /// Create a new resource with a given Uri.
        /// </summary>
        /// <param name="uri"></param>
        public GraphDbResource(Uri uri) : base(uri) { }
        
        /// <summary>
        /// Create a new resource with a given string. Throws an exception if string is Uri compatible.
        /// </summary>
        /// <param name="uriString">The string converted to a Uri. Throws an exception if not possible.</param>
        public GraphDbResource(string uriString)  : base(uriString) { }

        
        public void Persist(IGraphDbTransaction transaction)
        {
            if (Model != null && IsReadOnly == false)
            {
                // Update Resource in Model
                Model.UpdateResource(this, transaction);
            }
        }

        public void Reload(IGraphDbTransaction transaction)
        {
            
        }
         
    }
}
