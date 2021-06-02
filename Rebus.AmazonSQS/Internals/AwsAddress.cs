using System;
using Amazon;

namespace Rebus.Internals
{
    internal enum AwsAddressType
    {
        Unknown,
        NonQualifiedName,
        Arn,
        Url
    }

    internal enum AwsServiceType
    {
        Unknown,
        Sqs,
        Sns
    }

    internal class AwsAddress
    {
        public AwsAddressType AddressType { get; }
        public AwsServiceType ServiceType { get; }
        public string ResourceId { get; }
        public string FullAddress { get; }

        public AwsAddress(AwsAddressType addressType, AwsServiceType serviceType, string resourceId, string fullAddress)
        {
            AddressType = addressType;
            ServiceType = serviceType;
            ResourceId = resourceId;
            FullAddress = fullAddress;
        }

        public override string ToString()
        {
            return !string.IsNullOrEmpty(this.FullAddress)
                ? this.FullAddress
                : this.ResourceId;
        }

        public static AwsAddress Parse(string address)
        {
            if (!TryParse(address, out var awsAddress))
            {
                throw new InvalidOperationException($"Could not parse AwsAddress '{address}'");
            }

            return awsAddress;
        }

        public static bool TryParse(string address, out AwsAddress awsAddress)
        {
            if (Arn.IsArn(address))
            {
                awsAddress = FromArn(address);
                return awsAddress != null;
            }

            if (address.Contains("://"))
            {
                awsAddress = FromUrl(address);
                return awsAddress != null;
            }

            // Specified name is neither an ARN or URL, assume it's an nonqualified resource name
            awsAddress = FromNonQualifiedName(address);
            return true;
        }

        public static AwsAddress FromArn(string arnStr)
        {
            if (!Arn.TryParse(arnStr, out var arn))
            {
                // Unparsable ARN
                return null;
            }

            return new AwsAddress(
                AwsAddressType.Arn,
                ParseServiceType(arn.Service),
                arn.Resource,
                arnStr);
        }

        public static AwsAddress FromUrl(string urlStr)
        {
            if (!Uri.IsWellFormedUriString(urlStr, UriKind.Absolute))
            {
                // Malformed URL
                return null;
            }

            var uri = new Uri(urlStr);
            var hostParts = uri.Host.Split(':');
            var pathParts = uri.LocalPath.Split('/');

            return new AwsAddress(
                AwsAddressType.Url,
                ParseServiceType(hostParts[0]),
                pathParts[pathParts.Length - 1],
                urlStr);
        }

        public static AwsAddress FromNonQualifiedName(string name)
        {
            return new AwsAddress(AwsAddressType.NonQualifiedName, AwsServiceType.Unknown, name, name);
        }

        public static AwsServiceType ParseServiceType(string str)
        {
            switch (str.ToLower())
            {
                case "sqs":
                    return AwsServiceType.Sqs;

                case "sns":
                    return AwsServiceType.Sns;

                default:
                    return AwsServiceType.Unknown;
            }
        }
    }
}
