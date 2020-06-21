using System;
using System.Collections.Generic;
using System.Net.Http;
using System.Threading.Tasks;

using NknSdk.Common.Protobuf;
using NknSdk.Common.Protobuf.Transaction;
using NknSdk.Common.Rpc.Results;
using NknSdk.Common.Exceptions;
using NknSdk.Common.Extensions;
using NknSdk.Common.Options;
using NknSdk.Wallet;
using NknSdk.Wallet.Models;
using Newtonsoft.Json;
using System.Text;

namespace NknSdk.Common.Rpc
{
    public class RpcClient
    {
        private static HttpClient httpClient;

        static RpcClient()
        {
            httpClient = new HttpClient() { Timeout = TimeSpan.FromSeconds(10) };
        }

        public static GetWsAddressResult GetWsAddress(string nodeUri, string address)
        {
            address.ThrowIfNullOrEmpty("remoteAddress is empty");

            return CallRpc<GetWsAddressResult>(nodeUri, "getwsaddr", new { address });
        }

        public static GetWsAddressResult GetWssAddress(string nodeUri, string address)
        {
            address.ThrowIfNullOrEmpty("remoteAddress is empty");

            return CallRpc<GetWsAddressResult>(nodeUri, "getwssaddr", new { address });
        }

        public static GetSubscribersWithMetadataResult GetSubscribersWithMetadata(
            string nodeUri,
            string topic,
            int offset = 0,
            int limit = 1000,
            bool txPool = false)
        {
            topic.ThrowIfNullOrEmpty("topic is empty");

            var parameters = new
            {
                topic,
                offset,
                limit,
                meta = true,
                txPool
            };

            return CallRpc<GetSubscribersWithMetadataResult>(nodeUri, "getsubscribers", parameters);
        }

        public static GetSubscribersResult GetSubscribers(
            string nodeUri,
            string topic,
            int offset = 0,
            int limit = 1000,
            bool txPool = false)
        {
            topic.ThrowIfNullOrEmpty("topic is empty");

            var parameters = new
            {
                topic,
                offset,
                limit,
                meta = false,
                txPool
            };

            return CallRpc<GetSubscribersResult>(nodeUri, "getsubscribers", parameters);
        }

        public static int GetSubscribersCount(string nodeUri, string topic)
        {
            topic.ThrowIfNullOrEmpty("topic is empty");

            var parameters = new { topic };

            return CallRpc<int>(nodeUri, "getsubscriberscount", parameters);
        }

        public static GetSubscriptionResult GetSubscription(string nodeUri, string topic, string subscriber)
        {
            topic.ThrowIfNullOrEmpty("topic is empty");
            subscriber.ThrowIfNullOrEmpty("subscriber is empty");

            var parameters = new { topic, subscriber };

            return CallRpc<GetSubscriptionResult>(nodeUri, "getsubscription", parameters);
        }

        public static GetBalanceResult GetBalanceByAddress(string nodeUri, string address)
        {
            address.ThrowIfNullOrEmpty("remoteAddress is empty");

            var parameters = new { address };

            var rawResult = CallRpc<GetBalanceRpcResult>(nodeUri, "getbalancebyaddr", parameters);

            return new GetBalanceResult { Amount = decimal.Parse(rawResult.Amount), Address = address };
        }

        public static GetNonceByAddrResult GetNonceByAddress(string nodeUri, string address)
        {
            address.ThrowIfNullOrEmpty("remoteAddress is empty");

            var parameters = new { address };

            return CallRpc<GetNonceByAddrResult>(nodeUri, "getnoncebyaddr", parameters);
        }

        public static GetRegistrantResult GetRegistrant(string nodeUri, string name)
        {
            name.ThrowIfNullOrEmpty("name is empty");

            var parameters = new { name };

            return CallRpc<GetRegistrantResult>(nodeUri, "getregistrant", parameters);
        }

        public static GetLatestBlockHashResult GetLatestBlockHash(string nodeUri)
        {
            return CallRpc<GetLatestBlockHashResult>(nodeUri, "getlatestblockhash");
        }

        public static string SendRawTransaction(string nodeUri, Transaction transaction)
        {
            var bytes = transaction.ToBytes();

            var parameters = new { tx = bytes.ToHexString() };

            return CallRpc<string>(nodeUri, "sendrawtransaction", parameters);
        }

        public static string TransferTo(
            string toAddress, 
            Amount amount, 
            ITransactionSender transactionSender, 
            TransactionOptions options)
        {
            if (Address.Verify(toAddress) == false)
            {
                throw new Exception();
            }

            var nonce = options.Nonce ?? (transactionSender.GetNonceAsync()).Nonce.GetValueOrDefault();
            
            var signatureRedeem = Address.PublicKeyToSignatureRedeem(transactionSender.PublicKey);
            
            var programHash = Address.HexStringToProgramHash(signatureRedeem);
            
            var payload = TransactionFactory.MakeTransferPayload(programHash, Address.ToProgramHash(toAddress), amount.Value);
            
            var tx = transactionSender.CreateTransaction(payload, nonce, options);

            return transactionSender.SendTransactionAsync(tx);
        }

        public static string RegisterName(
            string name, 
            ITransactionSender transactionSender, 
            TransactionOptions options)
        {
            var nonce = options.Nonce ?? (transactionSender.GetNonceAsync()).Nonce.GetValueOrDefault();            
            
            var payload = TransactionFactory.MakeRegisterNamePayload(transactionSender.PublicKey, name, options.Fee.GetValueOrDefault());            
            
            var tx = transactionSender.CreateTransaction(payload, nonce, options);

            return transactionSender.SendTransactionAsync(tx);
        }

        public static string TransferName(
            string name, 
            string recipient, 
            ITransactionSender transactionSender,
            TransactionOptions options)
        {
            var nonce = options.Nonce ?? (transactionSender.GetNonceAsync()).Nonce.GetValueOrDefault();            
            
            var payload = TransactionFactory.MakeTransferNamePayload(name, transactionSender.PublicKey, recipient);            
            
            var tx = transactionSender.CreateTransaction(payload, nonce, options);

            return transactionSender.SendTransactionAsync(tx);
        }

        public static string DeleteName(
            string name, 
            ITransactionSender transactionSender,
            TransactionOptions options)
        {
            var nonce = options.Nonce ?? (transactionSender.GetNonceAsync()).Nonce.GetValueOrDefault();
            
            var payload = TransactionFactory.MakeDeleteNamePayload(transactionSender.PublicKey, name);
            
            var tx = transactionSender.CreateTransaction(payload, nonce, options);

            return transactionSender.SendTransactionAsync(tx);
        }

        public static string Subscribe(
            string topic, 
            int duration, 
            string identifier, 
            string meta,
            ITransactionSender transactionSender,
            TransactionOptions options)
        {
            var nonce = options.Nonce ?? (transactionSender.GetNonceAsync()).Nonce.GetValueOrDefault();
            
            var payload = TransactionFactory.MakeSubscribePayload(transactionSender.PublicKey, identifier, topic, duration, meta);
            
            var tx = transactionSender.CreateTransaction(payload, nonce, options);

            return transactionSender.SendTransactionAsync(tx);
        }

        public static string Unsubscribe(
            string topic,
            string identifier,
            ITransactionSender transactionSender,
            TransactionOptions options)
        {
            var nonce = options.Nonce ?? (transactionSender.GetNonceAsync()).Nonce.GetValueOrDefault();
            
            var payload = TransactionFactory.MakeUnsubscribePayload(transactionSender.PublicKey, topic, identifier);
            
            var tx = transactionSender.CreateTransaction(payload, nonce, options);

            return transactionSender.SendTransactionAsync(tx);
        }

        private static T CallRpc<T>(string nodeUri, string method, object parameters = null)
        {
            nodeUri.ThrowIfNullOrEmpty("address is empty");
            method.ThrowIfNullOrEmpty("method is empty");

            if (parameters == null)
            {
                parameters = new { };
            }

            var values = new Dictionary<string, object>
            {
                { "id", "nkn-sdk-js" },
                { "jsonrpc", 2.0 },
                { "method", method },
                { "params", parameters }
            };

            var data = Encoding.UTF8.GetBytes(JsonConvert.SerializeObject(values));

            var requestContent = new ByteArrayContent(data);

            var response = httpClient.PostAsync(nodeUri, requestContent).GetAwaiter().GetResult();
            if (response.IsSuccessStatusCode == false)
            {
                throw new ServerException($"Unsuccessful Rpc call. Node uri: {nodeUri} | Method: {method}");
            }

            var responseContent = response.Content.ReadAsStringAsync().GetAwaiter().GetResult();
            if (string.IsNullOrWhiteSpace(responseContent))
            {
                throw new ServerException("rpc response is empty");
            }

            var rpcResponse = JsonConvert.DeserializeObject<RpcResponse<T>>(responseContent);
            if (rpcResponse.IsSuccess == false)
            {
                throw new ServerException(rpcResponse.Error.Data);
            }

            if (rpcResponse.Result != null)
            {
                return rpcResponse.Result;
            }

            throw new InvalidResponseException("rpc response contains no result or error field");
        }

        public class GetBalanceRpcResult
        {
            public string Amount { get; set; }
        }
    }
}
