using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Collections.Concurrent;
using System.ServiceModel.Web;
using System.ServiceModel;
using System.ServiceModel.Description;
using System.Net;
using FlightSearchServer;

namespace Registeration
{
    public class AirSellerRegisteration : ITicketSellerRegisteration
    {
        private void RemoveSellerIfExists(Uri request, string name)
        {
            if (FlightSearchLogic.Instance.delegates.ContainsKey(name))
            {
                ITicketSellingQueryService tsqs;
                bool gotValue = FlightSearchLogic.Instance.delegates.TryGetValue(name, out tsqs);
                if (gotValue)
                {
                    IClientChannel currChannel = (IClientChannel)tsqs;
                    try
                    {
                        currChannel.Close();
                        currChannel.Abort();
                    }
                    catch (Exception)
                    {
                        currChannel.Abort();
                        Console.WriteLine("Closing of stale channel {0} failed, ignoring", currChannel.RemoteAddress.Uri.ToString());
                    }
                    ITicketSellingQueryService victimChannel;
                    FlightSearchLogic.Instance.delegates.TryRemove(name, out victimChannel);
                    Console.WriteLine("Successfully remove old seller {0} by name", name);
                }
            }


            foreach (var seller in FlightSearchLogic.Instance.sellers)
            {
                IClientChannel currChannel = ((IClientChannel)seller.Value);

                if (currChannel.RemoteAddress.Uri.Equals(request))
                {
                    Console.WriteLine("Detected connection retry by {0} from {1} ,removing old connection", name, request.ToString());
                    // Close this channel
                    try
                    {
                        currChannel.Close();
                        currChannel.Abort();
                    }
                    catch (Exception)
                    {
                        currChannel.Abort();
                        Console.WriteLine("Closing of stale channel {0} failed, ignoring", currChannel.RemoteAddress.Uri.ToString());
                    }
                    ITicketSellingQueryService victimChannel;
                    FlightSearchLogic.Instance.sellers.TryRemove(seller.Key, out victimChannel);
                }
            }

        }


        public void RegisterSeller(Uri request, string name)
        {
            RemoveSellerIfExists(request, name);

            //SVCUTIL::
            //ChannelFactory<ITicketSellingQueryService> httpFactory = new ChannelFactory<ITicketSellingQueryService>(new ServiceEndpoint(ContractDescription.GetContract(typeof(ITicketSellingQueryService)), new BasicHttpBinding(), new EndpointAddress(request)));
            // create channel proxy for endpoint
            //ITicketSellingQueryService channel = httpFactory.CreateChannel();
            //FlightSearchLogic.Instance.sellers[name] = channel;
            Console.WriteLine("seller {0} from {1} registered successfully", name, request.ToString());

        }
    }
}
