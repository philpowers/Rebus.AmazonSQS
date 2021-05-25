using Newtonsoft.Json;

namespace Rebus.AmazonSQS
{
    class AmazonTransportMessageSerializer
    {
        public string Serialize(AmazonTransportMessage message)
        {
            return JsonConvert.SerializeObject(message);
        }

        public AmazonTransportMessage Deserialize(string value)
        {
            if (value == null) return null;
            return JsonConvert.DeserializeObject<AmazonTransportMessage>(value);
        }
    }
}
