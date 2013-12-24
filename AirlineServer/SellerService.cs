using System;
using System.Collections.Generic;
using System.Linq;
using System.ServiceModel;
using System.ServiceModel.Description;
using System.ServiceModel.Web;
using System.Text;
using System.Threading.Tasks;

namespace AirlineServer
{
    class SellerService : ISellerService
    {
        public List<AirlineServer.ISellerService.Trip> getTrips(string src, string dst, DateTime date, List<string> sellers)
        {
            Dictionary<string, Uri> machines = new Dictionary<string, Uri>();
            List<AirlineServer.ISellerService.Flight> sourceFlights = new List<ISellerService.Flight>();
            foreach (string seller in sellers)
            {
                try
                {
                    ServiceEndpoint endPoint = new ServiceEndpoint(
                        ContractDescription.GetContract(typeof(ISellerClusterService)), new BasicHttpBinding(), new EndpointAddress(machines[seller]));
                    using (ChannelFactory<ISellerClusterService> httpFactory = new ChannelFactory<ISellerClusterService>(endPoint))
                    {
                        ISellerClusterService sellerCluster = httpFactory.CreateChannel();
                        sourceFlights.AddRange(sellerCluster.getRelevantFlightsBySrc(src, date));
                    }
                }
                catch (Exception) { }

            }
            return null;

        }
    }
}
