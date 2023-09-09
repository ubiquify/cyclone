export const largeArray = (size: number, value: number): Uint8Array => {
  const array = new Uint8Array(size);
  for (let i = 0; i < size; i++) {
    array[i] = value;
  }
  return array;
};

export const getCertificate = (): { key: string; cert: string } => {
  const key = process.env.UBIQ_KEY;
  const cert = process.env.UBIQ_CERT;
  if (key && cert) {
    return { key, cert };
  } else throw new Error("No certificate found");
};
