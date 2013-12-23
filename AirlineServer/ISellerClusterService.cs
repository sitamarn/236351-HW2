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
        AirlineServer.ISellerService.Seller getSellerClone(string seller);

        [OperationContract]
        List<AirlineServer.ISellerService.Flight> getRelevantFlightsBySrc(string src, DateTime date);

        [OperationContract]
        List<AirlineServer.ISellerService.Flight> getRelevantFlightsByDst(string dst, DateTime date);
    }
}
