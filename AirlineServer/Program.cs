using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.ServiceModel;
using System.Text;
using System.Threading.Tasks;

namespace AirlineServer
{
    class Program
    {
        static AirlineServer.ISellerService.Seller readSellerFile(string filePath, string sellerName)
        {
            List<AirlineServer.ISellerService.Flight> flights = new List<ISellerService.Flight>();
            StreamReader reader = new StreamReader(filePath);
            string line = reader.ReadLine();
            while (line != null)
            {
                AirlineServer.ISellerService.Flight flight = new ISellerService.Flight();
                string[] tokens = line.Split(' ');
                flight.flightNumber= tokens[0];
                flight.src= tokens[1];
                flight.dst= tokens[2];
                flight.date= DateTime.Parse(tokens[3]);
                flight.price= Convert.ToInt32(tokens[4]);
                flights.Add(flight);
            }
            AirlineServer.ISellerService.Seller seller = new ISellerService.Seller();
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
            AirlineServer.ISellerService.Seller seller = null;
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

            try
            {
                using (ServiceHost sellerHost = new ServiceHost(
                    typeof(SellerService), new Uri(sellerAddress)))
                {
                    using (ServiceHost intraHost = new ServiceHost(typeof(ISellerClusterService), new Uri(intraClusterAddress)))
                    {
                        // Create SOAP client
                        sellerHost.AddServiceEndpoint(typeof(ISellerService), new BasicHttpBinding(), "ISellerService");
                        intraHost.AddServiceEndpoint(typeof(ISellerClusterService), new BasicHttpBinding(), "ISellerClusterService");
                        // Open the service

                        sellerHost.Open();
                        intraHost.Open();
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
