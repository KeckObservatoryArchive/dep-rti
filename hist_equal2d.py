"""
Modified from skwok's hist_equal2d.py in
https://github.com/Keck-DataReductionPipelines/KeckDRPFramework/blob/develop/keckdrpframework/primitives/hist_equal2d.py
"""

import math
import numpy as np


class HistEqual2d():
    """
    Histogram equalization.
    """

    def __init__(self):
    	pass

    def _remap(self, arr, from_lo, from_hi, to_lo, to_hi):
        if from_hi == from_lo:
            a = np.empty_like(arr)
            a.fill(np.int_(to_lo))
            return a
        else:
            m = (to_hi - to_lo) / (from_hi - from_lo)
            b = -m * from_lo + to_lo
            return np.clip(np.int_(arr * m + b), to_lo, to_hi)

    def _centroid(self, data):
        """
        One step 1D centroiding algo.
        Returns centroid position and standard deviation
        """
        l = len(data)
        ixs = np.arange(l)
        ixs2 = ixs * ixs
        sumarr = np.sum(data)
        if sumarr == 0:
            return l / 2, 0
        cen = np.dot(data, ixs) / sumarr
        var = np.dot(data, ixs2) / sumarr - cen * cen
        return cen, math.sqrt(max(0, var))

    def _applyAHEqHelper(self, data, leng, from_lo, from_hi, to_lo, to_hi, n_hist, thold):
        """
        Adaptive histogram equalization
        """
        data1 = self._remap(data, from_lo, from_hi, to_lo, to_hi)
        histg, edges = np.histogram(data1, bins=n_hist, density=False)

        sumb4 = np.sum(histg)
        histg = np.clip(histg, 0, thold)
        hsum = np.cumsum(histg)
        ramp = np.linspace(0, (sumb4 - hsum[-1]), n_hist)
        hsum += ramp
        hsum = self._remap(hsum, hsum[0], hsum[-1], 0, 255)
        return hsum[np.int_(data1)]

    def _applyAHEC(self, img):
        cut_width = self.cut_width
        n_hist = self.n_hist
        flatData = img.flatten()
        leng = len(flatData)
        histg, edges = np.histogram(flatData, bins=n_hist, density=False)
        histg[0] = 0
        cen, cstd = self._centroid(histg)
        wing = cut_width * cstd
        lo_idx = int(max(0, cen - wing))
        hi_idx = int(min(cen + wing, n_hist))
        from_lo = edges[lo_idx]
        from_hi = edges[hi_idx]
        self.cen = cen
        self.stdev = cstd

        thold = leng / n_hist
        return self._applyAHEqHelper(flatData, leng, from_lo, from_hi, 0, n_hist - 1, n_hist, thold)

    def _applyAHEq(self, img):
        n_hist = self.n_hist
        flatData = img.flatten()
        leng = len(flatData)
        histg, edges = np.histogram(flatData, bins=n_hist, density=False)
        from_lo = edges[0]
        from_hi = edges[-1]
        thold = leng / n_hist
        return self._applyAHEqHelper(flatData, leng, from_lo, from_hi, 0, n_hist - 1, n_hist, thold)

    def _perform(self, img, cut_width=3, n_hist=65536):

        self.cut_width = cut_width
        self.n_hist = n_hist

        h, w = img.shape
       	new_img = self._applyAHEC(img).reshape((h, w)).astype(dtype="uint8")
       	return new_img


