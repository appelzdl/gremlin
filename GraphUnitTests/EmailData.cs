using GraphGetStarted;
using System;
using Xunit;

namespace GraphUnitTests
{
    public class UnitTest1
    {
        [Fact]
        public void GetDataFromExcelReturnsData()
        {
            var emailData = new EmailData();
            var results = emailData.GetCSV();

            Assert.NotEmpty(results);
        }
    }
}
