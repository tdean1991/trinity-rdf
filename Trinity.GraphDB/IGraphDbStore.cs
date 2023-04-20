using System;
using System.Collections.Generic;
using System.IO;
using System.Text;
using VDS.RDF;
using VDS.RDF.Storage;

namespace Semiodesk.Trinity.Store.GraphDB
{
    public interface IGraphDbStore : IStore
    {
        IGraphDbTransaction BeginTransaction();

        new
        /// <summary>
        /// Adds a new model with the given uri to the storage. 
        /// </summary>
        /// <param name="uri">Uri of the model</param>
        /// <param name="transaction">Active transaction</param>
        /// <returns>Handle to the model</returns>
        IModel CreateModel(Uri uri);

        /// <summary>
        /// Removes model from the store.
        /// </summary>
        /// <param name="uri">Uri of the model which is to be removed.</param>
        /// <param name="transaction">Active transaction</param>
        void RemoveModel(Uri uri, IGraphDbTransaction transaction);

        /// <summary>
        /// Removes model from the store.
        /// </summary>
        /// <param name="model">Handle to the model which is to be removed.</param>
        /// <param name="transaction">Active transaction</param>
        void RemoveModel(IModel model, IGraphDbTransaction transaction);

        /// <summary>
        /// Gets a handle to a model in the store.
        /// </summary>
        /// <param name="uri">Uri of the model.</param>
        /// <param name="transaction">Active transaction</param>
        /// <returns></returns>
        IModel GetModel(Uri uri);

        /// <summary>
        /// Lists all models in the store.
        /// </summary>
        /// <param name="transaction">Active transaction</param>
        /// <returns>All handles to existing models.</returns>
        IEnumerable<IModel> ListModels(IGraphDbTransaction transaction);

        /// <summary>
        /// Loads a serialized graph from the given location into the current store. See allowed <see cref="RdfSerializationFormat">formats</see>.
        /// </summary>
        /// <param name="graphUri">Uri of the graph in this store</param>
        /// <param name="url">Location</param>
        /// <param name="format">Allowed formats</param>
        /// <param name="update">Pass false if you want to overwrite the existing data. True if you want to add the new data to the existing.</param>
        /// <param name="transaction">Active transaction</param>
        /// <returns></returns>
        Uri Read(Uri graphUri, Uri url, RdfSerializationFormat format, bool update, IGraphDbTransaction transaction = null);

        /// <summary>
        /// Loads a serialized graph from the given stream into the current store. See allowed <see cref="RdfSerializationFormat">formats</see>.
        /// </summary>
        /// <param name="stream">Stream containing a serialized graph</param>
        /// <param name="graphUri">Uri of the graph in this store</param>
        /// <param name="format">Allowed formats</param>
        /// <param name="update">Pass false if you want to overwrite the existing data. True if you want to add the new data to the existing.</param>
        /// <param name="leaveOpen">Leaves the stream open</param>
        /// <param name="transaction">Active transaction</param>
        /// <returns></returns>
        Uri Read(Stream stream, Uri graphUri, RdfSerializationFormat format, bool update, bool leaveOpen = false, IGraphDbTransaction transaction = null);

        /// <summary>
        /// Loads a serialized graph from the given string into the current store. See allowed <see cref="RdfSerializationFormat">formats</see>.
        /// </summary>
        /// <param name="content">String containing a serialized graph</param>
        /// <param name="graphUri">Uri of the graph in this store</param>
        /// <param name="format">Allowed formats</param>
        /// <param name="update">Pass false if you want to overwrite the existing data. True if you want to add the new data to the existing.</param>
        /// <param name="transaction">Active transaction</param>
        /// <returns></returns>
        Uri Read(string content, Uri graphUri, RdfSerializationFormat format, bool update, IGraphDbTransaction transaction = null);

        /// <summary>
        /// Writes a serialized graph to the given stream. See allowed <see cref="RdfSerializationFormat">formats</see>.
        /// </summary>
        /// <param name="fs">Stream to which the content should be written.</param>
        /// <param name="graphUri">Uri fo the graph in this store.</param>
        /// <param name="format">Allowed formats.</param>
        /// <param name="namespaces">Defines namespace to prefix mappings for the output.</param>
        /// <param name="baseUri">Base URI for shortening URIs in formats that support it.</param>
        /// <param name="leaveOpen">Indicates if the stream should be left open after writing completes.</param>
        /// <param name="transaction">Active transaction</param>
        /// <returns></returns>
        void Write(Stream fs, Uri graphUri, RdfSerializationFormat format, INamespaceMap namespaces = null, Uri baseUri = null, bool leaveOpen = false, IGraphDbTransaction transaction = null);

        /// <summary>
        /// Writes a serialized graph to the given stream. See allowed <see cref="RdfSerializationFormat">formats</see>.
        /// </summary>
        /// <param name="fs">Stream to which the content should be written.</param>
        /// <param name="graphUri">Uri fo the graph in this store.</param>
        /// <param name="formatWriter">A RDF writer.</param>
        /// <param name="leaveOpen">Indicates if the stream should be left open after writing completes.</param>
        /// <param name="transaction">Active transaction</param>
        /// <returns></returns>
        void Write(Stream fs, Uri graphUri, IRdfWriter formatWriter, bool leaveOpen = false, IGraphDbTransaction transaction = null);

    }
}
