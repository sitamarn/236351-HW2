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
        public List<AirlineServer.Trip> getTrips(string src, string dst, DateTime date, List<string> sellers)
        {
            List<string> sellersToSearch;
            Dictionary<string, Uri> machines = //AirlineReplicationModule.getAirlineReplicationModule().getSellersAndPrimaries();
            List<Flight> sourceFlights = new List<Flight>();
            List<Flight> dstFlights = new List<Flight>();
            List<Trip> trips = new List<Trip>();
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
                
                foreach (AirlineServer.Flight srcFlight in sourceFlights)
                {
                    if (srcFlight.dst.Equals(dst))
                    {
                        Trip trip = new Trip();
                        trip.firstFlight = srcFlight;
                        trip.secondFlight = null;
                        trip.price = srcFlight.price;
                        trips.Add(trip);
                        continue;
                    }
                    foreach (AirlineServer.Flight dstFlight in dstFlights)
                    {
                        if (srcFlight.dst.Equals(dstFlight.src))
                        {
                            Trip trip = new Trip();
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
