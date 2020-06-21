using System.Threading.Tasks;

using NknSdk.Common.Options;
using NknSdk.Common.Protobuf.Transaction;
using NknSdk.Common.Rpc.Results;

namespace NknSdk.Common
{
    public interface ITransactionSender
    {
        string PublicKey { get; }

        GetNonceByAddrResult GetNonceAsync();

        Transaction CreateTransaction(Payload payload, long nonce, TransactionOptions options);

        string SendTransactionAsync(Transaction tx);
    }
}
