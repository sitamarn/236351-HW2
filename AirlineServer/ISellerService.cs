﻿using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Runtime.Serialization;
using System.ServiceModel;

namespace AirlineServer
{
    [ServiceContract]
    interface ISellerService
    {
        [OperationContract]
        List<Trip> getTrips(string src, string dst, DateTime date, List<string> sellers);
    }
        /// <summary>
        /// A single Flight search query result. this class will be contained within the list
        /// QueryResultFlights.
        /// </summary>
        [DataContract]
        public class Flight
        {
            [DataMember]
            public int price { get; set; }
            [DataMember]
            public string flightNumber { get; set; }
            [DataMember]
            public string src { get; set; }
            [DataMember]
            public string dst { get; set; }
            [DataMember]
            public DateTime date { get; set; }
            [DataMember]
            public string seller { get; set; }
        }

        [DataContract]
        public class Trip
        {
            [DataMember]
            public int price { get; set; }
            [DataMember]
            public Flight firstFlight { get; set; }
            [DataMember]
            public Flight secondFlight { get; set; }
        }

        [DataContract]
        public class Seller
        {
            [DataMember]
            public List<Flight> flights { get; set; }
            [DataMember]
            public string name { get; set; }
            [DataMember]
            public int version { get; set; }
        }

    }

