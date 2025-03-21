package org.howietkl.sqlite;

import java.nio.ByteBuffer;

public class Utils {

  public static long getVarint(ByteBuffer buf) {
    byte b;
    if ((b = buf.get()) >= 0) {
      return b;
    }
    long result = (b & 0b01111111) << 7;
    if ((b = buf.get()) >= 0) {
      return result | (b & 0b01111111);
    } else {
      result |= (b & 0b01111111);
      result <<= 7;
      if ((b = buf.get()) >= 0) {
        return result | (b & 0b01111111);
      } else {
        result |= (b & 0b01111111);
        result <<= 7;
        if ((b = buf.get()) >= 0) {
          return result | (b & 0b01111111);
        } else {
          result |= (b & 0b01111111);
          result <<= 7;
          if ((b = buf.get()) >= 0) {
            return result | (b & 0b01111111);
          } else {
            result |= (b & 0b01111111);
            result <<= 7;
            if ((b = buf.get()) >= 0) {
              return result | (b & 0b01111111);
            } else {
              result |= (b & 0b01111111);
              result <<= 7;
              if ((b = buf.get()) >= 0) {
                return result | (b & 0b01111111);
              } else {
                result |= (b & 0b01111111);
                result <<= 7;
                if ((b = buf.get()) >= 0) {
                  return result | (b & 0b01111111);
                } else {
                  result |= (b & 0b01111111);
                  result <<= 7;
                  if ((b = buf.get()) >= 0) {
                    return result | (b & 0b01111111);
                  } else {
                    result |= (b & 0b01111111);
                    result <<= 7;
                    if ((b = buf.get()) >= 0) {
                      return result | (b & 0b01111111);
                    } else {
                      throw new UnsupportedOperationException();
                    }
                  }
                }
              }
            }
          }
        }
      }
    }
  }

}
