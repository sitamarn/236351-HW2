using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.ServiceModel;
using System.ServiceModel.Description;
using System.ServiceModel.Web;
using System.Text;
using System.Threading.Tasks;
using Registeration;
using System.Net;

namespace AirlineServer
{
    class Program
    {
        static AirlineServer.Seller readSellerFile(string filePath, string sellerName)
        {
            List<AirlineServer.Flight> flights = new List<Flight>();
            StreamReader reader = new StreamReader(filePath);
            string line = reader.ReadLine();
            while (line != null)
            {
                AirlineServer.Flight flight = new Flight();
                string[] tokens = line.Split(' ');
                flight.flightNumber = tokens[0];
                flight.src = tokens[1];
                flight.dst = tokens[2];
                flight.seller = sellerName;
                flight.date = DateTime.Parse(tokens[3]);
                flight.price = Convert.ToInt32(tokens[4]);
                flights.Add(flight);
                line = reader.ReadLine();
            }
            reader.Close();
            AirlineServer.Seller seller = new Seller();
            seller.name = sellerName;
            seller.flights = flights;
            return seller;
        }
        static void Main(string[] args)
        {
            if (args.Length != 6)
            {
                Console.WriteLine("Bad arguments");
                Console.WriteLine("TicketSellingServer.exe <name> <alliance> <search server port> <airline servers port> <flights search server URI #2> <input file>");
                return;
            }
            string url = null;
            AirlineServer.Seller seller = null;
            // Check the input:
            try
            {
                url = @"http://" + args[4];
                Convert.ToInt32(args[2]);
                Convert.ToInt32(args[3]);
                new Uri(url);
                seller = readSellerFile(args[5], args[0]);
            }
            catch (Exception e)
            {
                Console.WriteLine("Bad arguments: " + e.Message);
                return;
            }


            // Read arguments
            string intraClusterAddress = @"http://localhost:" + args[3] + @"/IntraClusterService";
            string sellerAddress = @"http://localhost:" + args[2] + @"/SellerService";
            //TicketSellingQueryLogic.Instance.Initialize(args[2], args[3]);
            SellerService sa = new SellerService();
            IntraClusterService ics = new IntraClusterService(seller);
            try
            {
                using (ServiceHost sellerHost = new ServiceHost(sa, new Uri(sellerAddress)))
                {
                    using (ServiceHost intraHost = new ServiceHost(ics, new Uri(intraClusterAddress)))
                    {


                        ServiceEndpoint sellerEndPoint = sellerHost.AddServiceEndpoint(typeof(ISellerService), new BasicHttpBinding(), sellerAddress);
                        // add http get support
                        ServiceMetadataBehavior Ismb = new ServiceMetadataBehavior();
                        Ismb.HttpGetEnabled = true;
                        sellerHost.Description.Behaviors.Add(Ismb);
                        sellerHost.Description.Behaviors.Find<ServiceBehaviorAttribute>().InstanceContextMode = InstanceContextMode.Single;

                        ServiceEndpoint endPoint = intraHost.AddServiceEndpoint(typeof(ISellerClusterService), new BasicHttpBinding(), intraClusterAddress);




                        // add http get support
                        ServiceMetadataBehavior smb = new ServiceMetadataBehavior();
                        smb.HttpGetEnabled = true;

                        intraHost.Description.Behaviors.Add(smb);
                        intraHost.Description.Behaviors.Find<ServiceBehaviorAttribute>().InstanceContextMode = InstanceContextMode.Single;



                        // Create SOAP client
                        
                        
                        // Open the service
                        sellerHost.Open();
                        intraHost.Open();

                        try
                        {
                            WebChannelFactory<IAirSellerRegisteration> cf = new WebChannelFactory<IAirSellerRegisteration>(new Uri(url));
                            IAirSellerRegisteration registerChannel = cf.CreateChannel();
                            // Register the channel in the server
                            registerChannel.RegisterSeller(new Uri(sellerAddress), args[1]);
                            
                        }
                        catch (ProtocolException e)
                        {
                            Console.WriteLine("Bad Protocol: " + e.Message);
                        }
                        catch (Exception e)
                        {

                            if (e.InnerException is WebException)
                            {
                                HttpWebResponse resp = (HttpWebResponse)((WebException)e.InnerException).Response;
                                Console.WriteLine("Failed, {0}", resp.StatusDescription);
                            }
                            else
                            {
                                Console.WriteLine("Advertisement connection kicked the bucket, quitting because:");
                                Console.WriteLine(e.Message.ToString());
                            }
                            return;
                        }

                        // Keeping the service alive till pressing ENTER
                        Console.ReadKey();
                   }
                }
            }

            catch (Exception e)
            {
                Console.WriteLine("Failed executing because:");
                Console.WriteLine(e.Message);
            }
        }
    
    }
}
