const xml2js = require('xml2js');

async function parseSoapResponse(xml) {
  const parser = new xml2js.Parser({
    explicitArray: false,
    ignoreAttrs: true
  });

  const result = await parser.parseStringPromise(xml);

  // Navigate typical .asmx DataSet structure
  try {
    const dataSet =
      result['soap:Envelope']['soap:Body']
        .GetResponseAsDataSetResponse
        .GetResponseAsDataSetResult
        .diffgr.diffgram.NewDataSet;

    return dataSet;
  } catch (err) {
    return result;
  }
}

module.exports = {
  parseSoapResponse
};
