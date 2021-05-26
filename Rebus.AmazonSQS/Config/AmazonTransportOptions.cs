using System;
using Amazon.Runtime;

namespace Rebus.Config
{
    public abstract class AmazonTransportOptions<TClient> where TClient : IAmazonService
    {
        /// <summary>
        /// Function that gets a new instance of the specific <see cref="IAmazonService"/> used for the transport
        /// </summary>
        public Func<TClient> ClientFactory;
    }
}