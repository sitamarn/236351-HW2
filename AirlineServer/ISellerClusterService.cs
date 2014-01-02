using System;
using System.Collections.Generic;
using System.Linq;
using System.ServiceModel;
using System.Text;
using System.Threading.Tasks;

namespace AirlineServer
{
    [ServiceContract]
    interface ISellerClusterService
    {
        [OperationContract]
        Seller sendPrimarySeller(string sellerName);

        [OperationContract]
        Seller sendBackupSeller(string sellerName);

        [OperationContract]
        List<Flight> getRelevantFlightsBySrc(string src, DateTime date, List<string> sellersToSearch);

        [OperationContract]
        List<Flight> getRelevantFlightsByDst(string dst, DateTime date, List<string> sellersToSearch);
    }
}
