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
        AirlineServer.Seller getSellerClone(string seller);

        [OperationContract]
        List<AirlineServer.Flight> getRelevantFlightsBySrc(string src, DateTime date);

        [OperationContract]
        List<AirlineServer.Flight> getRelevantFlightsByDst(string dst, DateTime date);
    }
}
