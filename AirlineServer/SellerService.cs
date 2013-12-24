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
            List<string> sellersToSearch;
            Dictionary<string, Uri> machines = AirlineReplicationModule.getAirlineReplicationModule().getSellersAndPrimaries();
            List<AirlineServer.ISellerService.Flight> sourceFlights = new List<ISellerService.Flight>();
            List<AirlineServer.ISellerService.Flight> dstFlights = new List<ISellerService.Flight>();
            List<AirlineServer.ISellerService.Trip> trips = new List<ISellerService.Trip>();
            if (sellers.Count == 0) { sellersToSearch = machines.Keys.ToList(); }
            else { sellersToSearch = sellers; }
            foreach (string seller in sellersToSearch)
            {
                try
                {
                    ServiceEndpoint endPoint = new ServiceEndpoint(
                        ContractDescription.GetContract(typeof(ISellerClusterService)), new BasicHttpBinding(), new EndpointAddress(machines[seller]));
                    using (ChannelFactory<ISellerClusterService> httpFactory = new ChannelFactory<ISellerClusterService>(endPoint))
                    {
                        ISellerClusterService sellerCluster = httpFactory.CreateChannel();
                        sourceFlights.AddRange(sellerCluster.getRelevantFlightsBySrc(src, date));
                        dstFlights.AddRange(sellerCluster.getRelevantFlightsByDst(dst, date));
                        dstFlights.AddRange(sellerCluster.getRelevantFlightsByDst(dst, date.AddDays(1)));
                    }
                }
                catch (Exception) { }
                
                foreach (AirlineServer.ISellerService.Flight srcFlight in sourceFlights)
                {
                    if (srcFlight.dst.Equals(dst))
                    {
                        ISellerService.Trip trip = new ISellerService.Trip();
                        trip.firstFlight = srcFlight;
                        trip.secondFlight = null;
                        trip.price = srcFlight.price;
                        trips.Add(trip);
                        continue;
                    }
                    foreach (AirlineServer.ISellerService.Flight dstFlight in dstFlights)
                    {
                        if (srcFlight.dst.Equals(dstFlight.src))
                        {
                            ISellerService.Trip trip = new ISellerService.Trip();
                            trip.firstFlight = srcFlight;
                            trip.secondFlight = dstFlight;
                            trip.price = srcFlight.price + dstFlight.price;
                            trips.Add(trip);
                        }
                    }
                }

                trips.Sort();
            }
            return trips;

        }
    }
}
