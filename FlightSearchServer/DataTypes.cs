using System;
using System.Collections.Generic;
using System.Linq;
using System.Runtime.Serialization;
using System.Web;

namespace FlightSearchServer
{

    /// <summary>
    /// Collection of Trips which were retrieved by search queries who where delegated
    /// to all the sellers.
    /// This list is propogated to the client as a result
    /// </summary>
    [CollectionDataContract]
    public class QueryResultTrips : List<QueryResultTrip>
    {

        public QueryResultTrips() { }
        public QueryResultTrips(List<QueryResultTrip> trip) : base(trip) { }
    }

    /// <summary>
    /// A single trip search query result. this class will be contained within the list
    /// QueryResultTrips.
    /// </summary>
    [DataContract]
    public class QueryResultTrip : IComparable
    {
        [DataMember]
        public int price { get; set; }
        [DataMember]
        public QueryResultFlight firstFlight { get; set; }
        [DataMember]
        public QueryResultFlight secondFlight { get; set; }

        /// <summary>
        /// This function is used to sort the list by spec requirement.
        /// Client is dumb so sorting is done on server
        /// </summary>
        /// <param name="obj">Right hand object to test</param>
        /// <returns>-1 if this is smaller than obj, otherwise 1</returns>
        public int CompareTo(object obj)
        {
            QueryResultTrip otherTrip = (QueryResultTrip)obj;

            if (price < otherTrip.price) { return -1; }
            else { return 1; }
        }

        /*
                public static explicit operator QueryResultFlight(TicketSellingServer.Flight sellerFlight)
                {
                    QueryResultFlight clientFlight = new QueryResultFlight();
                    clientFlight.dst = sellerFlight.dst;
                    clientFlight.src = sellerFlight.src;
                    clientFlight.seats = sellerFlight.seats;
                    clientFlight.price = sellerFlight.price;
                    clientFlight.name = "UNKNOWN_SELLER_WITH_A_REALLY_REALLY_LONG_NAME_WHICH_NOBODY_CARES_ABOUT";
                    clientFlight.flightNumber = sellerFlight.flightNumber;
                    clientFlight.date = sellerFlight.date;

                    return clientFlight;
                }
         * */
    }


    /// <summary>
    /// A single Flight search query result. this class will be contained within the list
    /// QueryResultFlights.
    /// </summary>
    [DataContract]
    public class QueryResultFlight : IComparable
    {
        [DataMember]
        public string price { get; set; }
        [DataMember]
        public string flightNumber { get; set; }
        [DataMember]
        public string src { get; set; }
        [DataMember]
        public string dst { get; set; }
        [DataMember]
        public string seller { get; set; }
        [DataMember]
        public DateTime date { get; set; }
    }

    /*
        public static explicit operator QueryResultFlight(TicketSellingServer.Flight sellerFlight)
        {
            QueryResultFlight clientFlight = new QueryResultFlight();
            clientFlight.dst = sellerFlight.dst;
            clientFlight.src = sellerFlight.src;
            clientFlight.seats = sellerFlight.seats;
            clientFlight.price = sellerFlight.price;
            clientFlight.name = "UNKNOWN_SELLER_WITH_A_REALLY_REALLY_LONG_NAME_WHICH_NOBODY_CARES_ABOUT";
            clientFlight.flightNumber = sellerFlight.flightNumber;
            clientFlight.date = sellerFlight.date;

            return clientFlight;
        }
     * */
}

