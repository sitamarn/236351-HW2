using System;
using System.Collections.Generic;
using System.Linq;
using System.ServiceModel;
using System.Text;
using System.Threading.Tasks;

namespace AirlineServer
{
    [ServiceContract]
    interface IReplicatedSeller
    {
        [OperationContract]
        void setMachineAsPrimary(string sellerName);

        [OperationContract]
        void setMachineAsBackup(string sellerName);

        [OperationContract]
        void resetCache(string sellerName);

        [OperationContract]
        void dropSeller(string sellerName);

        [OperationContract]
        Dictionary<string, int> getSellersAndTheirVersions();




    }
}
