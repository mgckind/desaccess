## plotutils.py

import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
import pickle
from matplotlib.patches import Ellipse
from astropy.wcs import WCS, _wcs
from matplotlib.patches import Rectangle

font = {'size':'18'}
#font2 = {'size':'12'}
font3 = {'size':'14'}

ARCSEC_TO_DEG = 0.000278

des = [['g','r','i','z','y'],[475,635,775,925,1000],[150,150,150,150,110],[400,560,700,850,945],[550,710,850,1000,1055]]    # [filter,cwl,fwhm,min,max] (nanometers)
vhs = [['J','H','Ks'],[1252,1645,2147],[172,291,309],[1166,1499.5,1992.5],[1338,1790.5,2301.5]]        # [filter,cwl,fwhm,min,max] (nanometers)
wise = [['w1','w2','w3','w4'],[3352.6,4602.8,11560.8,22088.3],[662.56,1042.3,5506.9,4101.3],[3021.32,4081.65,8807.35,20037.65],[3683.88,5123.95,14314.25,24138.95]]        # [filter,cwl,fwhm,min,max] (nanometers)



def DecConverter(ra, dec):
    ra1 = np.abs(ra/15)
    raHH = int(ra1)
    raMM = int((ra1 - raHH) * 60)
    raSS = (((ra1 - raHH) * 60) - raMM) * 60
    raSS = np.round(raSS, decimals=1)
    raOUT = '{0:02d}{1:02d}{2:04.1f}'.format(raHH, raMM, raSS) if ra > 0 else '-{0:02d}{1:02d}{2:04.1f}'.format(raHH, raMM, raSS)
    
    dec1 = np.abs(dec)
    decDD = int(dec1)
    decMM = int((dec1 - decDD) * 60)
    decSS = (((dec1 - decDD) * 60) - decMM) * 60
    decSS = np.round(decSS, decimals=1)
    decOUT = '-{0:02d}{1:02d}{2:04.1f}'.format(decDD, decMM, decSS) if dec < 0 else '+{0:02d}{1:02d}{2:04.1f}'.format(decDD, decMM, decSS)
    
    return(raOUT + decOUT)

def _eigsorted(cov):
    vals, vecs = np.linalg.eigh(cov)
    order = np.argsort(vals[::-1])            # [::-1] reverses the order of the array
    return(vals[order], vecs[:,order])

def PlotFluxVsWavelength(folder, df, a, b, fi, ex):
    """
    Make plot of Flux vs Wavelength.
    Inputs are a dataframe, and booleans a and b for which surveys to include in the plot.
    """
    
    fig = plt.figure()
    ax = fig.add_subplot(111)
    
    point1 = ax.scatter(des[1], df.loc[0]['G_FLUX':'Y_FLUX'], color='b', marker='^', facecolor='none', edgecolor='b')#, label='DES')
    ax.errorbar(des[1], df.loc[0]['G_FLUX':'Y_FLUX'], xerr=[x/2 for x in des[2]], yerr=df.loc[0]['G_FLUXERR':'Y_FLUXERR'], fmt='None', ecolor='b', linestyle='None')
    points = [point1]
    labels = ['DES']
    
    if a is True:
        point2 = ax.scatter(wise[1], df.loc[0]['W1FLUX':'W4FLUX'], color='r', marker='o', facecolor='none', edgecolor='r', zorder=10)#, label='WISE')
        ax.errorbar(wise[1], df.loc[0]['W1FLUX':'W4FLUX'], xerr=[x/2 for x in wise[2]], yerr=df.loc[0]['W1FLUXERR':'W4FLUXERR'], fmt='None', ecolor='r', linestyle='None', zorder=10)
        points.append(point2)
        labels.append('WISE')
        
        if df['W1SNR'][0] < 5:
            ax.annotate('', xy=(wise[1][0],df['W1FLUX'][0]*0.5), xytext=(wise[1][0],df['W1FLUX'][0]*(0.5/3)), arrowprops=dict(facecolor='r', edgecolor='r', arrowstyle='<|-'), zorder=10)
        if df['W2SNR'][0] < 5:
            ax.annotate('', xy=(wise[1][1],df['W2FLUX'][0]*0.5), xytext=(wise[1][1],df['W2FLUX'][0]*(0.5/3)), arrowprops=dict(facecolor='r', edgecolor='r', arrowstyle='<|-'), zorder=10)
        if df['W3SNR'][0] < 5:
            ax.annotate('', xy=(wise[1][2],df['W3FLUX'][0]*0.5), xytext=(wise[1][2],df['W3FLUX'][0]*(0.5/3)), arrowprops=dict(facecolor='r', edgecolor='r', arrowstyle='<|-'), zorder=10)
        if df['W4SNR'][0] < 5:
            ax.annotate('', xy=(wise[1][3],df['W4FLUX'][0]*0.5), xytext=(wise[1][3],df['W4FLUX'][0]*(0.5/3)), arrowprops=dict(facecolor='r', edgecolor='r', arrowstyle='<|-'), zorder=10)
        
    if b is True:
        point3 = ax.scatter(vhs[1], df.loc[0]['JFLUX':'KSFLUX'], color='g', marker='s', facecolor='none', edgecolor='g', zorder=10)#, label='VHS')
        ax.errorbar(vhs[1], df.loc[0]['JFLUX':'KSFLUX'], xerr=[x/2 for x in vhs[2]], yerr=df.loc[0]['JFLUXERR':'KSFLUXERR'], fmt='None', ecolor='g', linestule='None', zorder=10)
        points.append(point3)
        labels.append('VHS')
    
    ax.set_title(fi, **font)
    ax.set_xlabel('Wavelength (nm)', **font)
    ax.set_ylabel('Flux (mJy)', **font)
    ax.set_xscale('log')
    ax.set_yscale('log')
    ax.tick_params(axis='both', which='both', labelsize='18')
    ax.legend(points, labels, prop={'size':18}, scatterpoints=1)#, loc=2)
    plt.tight_layout()
    plt.savefig(folder+ '/' + fi + '_flux' + ex, dpi=300)

def PlotMagVsWavelength(folder, df, a, b, fi, ex):
    """
    Make plot of Magnitude vs Wavelength.
    Inputs are a dataframe, and booleans a and b for which surveys to include in the plot.
    """
    
    fig = plt.figure()
    ax = fig.add_subplot(111)
    
    point1 = ax.scatter(des[1], df.loc[0]['MAG_G':'MAG_Y'], color='b', marker='^', facecolor='none', edgecolors='b', zorder=10)#, label='DES')
    ax.errorbar(des[1], df.loc[0]['MAG_G':'MAG_Y'], xerr=[x/2 for x in des[2]], yerr=df.loc[0]['MAGERR_G':'MAGERR_Y'], fmt='None', ecolor='b', linestyle='None', zorder=10)
    points = [point1]
    labels = ['DES']
    
    if a is True:
        point2 = ax.scatter(wise[1], df.loc[0]['W1MPRO':'W4MPRO'], color='r', marker='o', facecolor='none', edgecolors='r', zorder=10)#, label='WISE')
        ax.errorbar(wise[1], df.loc[0]['W1MPRO':'W4MPRO'], xerr=[x/2 for x in wise[2]], yerr=df.loc[0]['W1SIGMPRO':'W4SIGMPRO'], fmt='None', ecolor='r', linestyle='None', zorder=10)
        points.append(point2)
        labels.append('WISE')
        
        if df['W1SNR'][0] < 5:
            ax.annotate('', xy=(wise[1][0],df['W1MPRO'][0]+1.5), xytext=(wise[1][0],df['W1MPRO'][0]+3.5), arrowprops=dict(facecolor='r', edgecolor='r', arrowstyle='<|-'), zorder=10)
        if df['W2SNR'][0] < 5:
            ax.annotate('', xy=(wise[1][1],df['W2MPRO'][0]+1.5), xytext=(wise[1][1],df['W2MPRO'][0]+3.5), arrowprops=dict(facecolor='r', edgecolor='r', arrowstyle='<|-'), zorder=10)
        if df['W3SNR'][0] < 5:
            ax.annotate('', xy=(wise[1][2],df['W3MPRO'][0]+1.5), xytext=(wise[1][2],df['W3MPRO'][0]+3.5), arrowprops=dict(facecolor='r', edgecolor='r', arrowstyle='<|-'), zorder=10)
        if df['W4SNR'][0] < 5:
            ax.annotate('', xy=(wise[1][3],df['W4MPRO'][0]+1.5), xytext=(wise[1][3],df['W4MPRO'][0]+3.5), arrowprops=dict(facecolor='r', edgecolor='r', arrowstyle='<|-'), zorder=10)
    
    if b is True:
        point3 = ax.scatter(vhs[1], df.loc[0]['JAPERMAG3':'KSAPERMAG3'], color='g', marker='s', facecolor='none', edgecolors='g', zorder=10)#, label='VHS')
        ax.errorbar(vhs[1], df.loc[0]['JAPERMAG3':'KSAPERMAG3'], xerr=[x/2 for x in vhs[2]], yerr=df.loc[0]['JAPERMAG3ERR':'KSAPERMAG3ERR'], fmt='None', ecolor='g', linestyle='None', zorder=10)
        points.append(point3)
        labels.append('VHS')
    
    ax.set_title(fi, **font)
    ax.set_xlabel('Wavelength (nm)', **font)
    ax.set_ylabel('Magnitude', **font)
    ax.set_xscale('log')
    ax.tick_params(axis='both', which='both', labelsize='18')
    ax.legend(points, labels, prop={'size':18}, scatterpoints=1)#, loc=1)
    plt.gcf().subplots_adjust(bottom=0.15)
    plt.tight_layout()
    plt.savefig(folder+ '/' + fi + '_magnitude' + ex, dpi=300) 

def PlotGR_RI(folder, df, fi, ex):
    seds = pd.DataFrame(pd.read_json('easyweb/static/plotdata/stellar_seds.json',orient='columns',typ='frame'))
    ot = ''
    s = 99999
    r = 0
    for row in range(len(seds['SPT'])):
        a = np.abs(df['G_R'][0] - seds['G_R_des'][row]) + np.abs(df['R_I'][0] - seds['R_I_des'][row])
        if a < s:
            s = a
            r = row
    ot = str(seds['SPT'][r])
    
    fig = pickle.load(open('easyweb/static/plotdata/grri_contours.pkl','rb'))
    ax = fig.add_subplot(111)
    ax.scatter(df['G_R'], df['R_I'], color='maroon', alpha=0.75, s=100, zorder=10)
    #ax.text(-0.4, 1.75, 'Suggested Object\nType: ' + ot, va='center', **font3)
    ax.text(0.02, 0.92, 'Suggested Object\nType: ' + ot, va='center', transform=ax.transAxes, **font3)
    
    cov = np.array([[df['MAGERR_G'][0]**2+df['MAGERR_R'][0]**2, -(df['MAGERR_R'][0]**2)], [-(df['MAGERR_R'][0]**2), df['MAGERR_R'][0]**2+df['MAGERR_I'][0]**2]])
    vals, vecs = _eigsorted(cov)
    theta = np.degrees(np.arctan2(*vecs[:,0][::-1]))
    
    ntsd1 = 1
    w1, h1 = 2 * ntsd1 * np.sqrt(vals)
    ell1 = Ellipse(xy=(df['G_R'][0], df['R_I'][0]), width=w1, height=h1, angle=theta, color='black', zorder=100)
    ell1.set_facecolor('none')
    ax.add_artist(ell1)

    ntsd2 = 2
    w2, h2 = 2 * ntsd2 * np.sqrt(vals)
    ell2 = Ellipse(xy=(df['G_R'][0], df['R_I'][0]), width=w2, height=h2, angle=theta, color='black', zorder=100)
    ell2.set_facecolor('none')
    ax.add_artist(ell2)
    
    xmax = 2.5
    ymax = 2.0
    if df['G_R'][0] > xmax:
        xmax = df['G_R'][0] + 0.5
    if df['R_I'][0] > ymax:
        ymax = df['R_I'][0] + 0.5
    xmin = -0.5
    ymin = -1.0
    if df['G_R'][0] < xmin:
        xmin = df['G_R'][0] - 0.5
    if df['R_I'][0] < ymin:
        ymin = df['R_I'][0] - 0.5
    
    ax.set_title(fi, **font)
    ax.set_xlabel(r'$g-r$', **font)
    ax.set_ylabel(r'$r-i$', **font)
    ax.set_xlim(xmin, xmax)
    ax.set_ylim(ymin, ymax)
    ax.tick_params(axis='both', which='both', labelsize='18')
    #plt.gcf().subplots_adjust(bottom=0.15)
    plt.savefig(folder+ '/' + fi + '_gr-ri' + ex, bbox_inches='tight', dpi=300)

def PlotGZ_ZW1(folder, df, fi, ex):
    ot = ''
    if df['Z_W1'][0] > ((2.0 * df['G_Z'][0]) + 1.5):
        ot = 'Quasar'
    elif (df['Z_W1'][0] <= ((2.0 * df['G_Z'][0]) + 1.5)) and (df['Z_W1'][0] > ((0.34 * df['G_Z'][0]) + 1.6)):
        ot = 'Galaxy'
    elif df['Z_W1'][0] <= ((0.34 * df['G_Z'][0]) + 1.6):
        ot = 'Star'
    else:
        ot = 'Unknown'
    
    fig = pickle.load(open('easyweb/static/plotdata/gzzw1_plot.pkl','rb'))
    ax = fig.add_subplot(111)
    ax.scatter(df['G_Z'][0], df['Z_W1'][0], color='maroon', alpha=0.75, s=100, zorder=10)
    #ax.text(3.0, 4.65, 'Suggested Object\nType: ' + ot, va='center', **font3)
    ax.text(0.63, 0.92, 'Suggested Object\nType: ' + ot, va='center', transform=ax.transAxes, **font3)
    
    cov = np.array([[df['MAGERR_G'][0]**2+df['MAGERR_Z'][0]**2, -(df['MAGERR_Z'][0]**2)], [-(df['MAGERR_Z'][0]**2), df['MAGERR_Z'][0]**2+df['W1SIGMPRO'][0]**2]])
    vals, vecs = _eigsorted(cov)
    theta = np.degrees(np.arctan2(*vecs[:,0][::-1]))
    
    ntsd1 = 1
    w1, h1 = 2 * ntsd1 * np.sqrt(vals)
    ell1 = Ellipse(xy=(df['G_Z'][0], df['Z_W1'][0]), width=w1, height=h1, angle=theta, color='black', zorder=100)
    ell1.set_facecolor('none')
    ax.add_artist(ell1)
    
    ntsd2 = 2
    w2, h2 = 2 * ntsd2 * np.sqrt(vals)
    ell2 = Ellipse(xy=(df['G_Z'][0], df['Z_W1'][0]), width=w2, height=h2, angle=theta, color='black', zorder=100)
    ell2.set_facecolor('none')
    ax.add_artist(ell2)
    
    xmax = 5.0
    ymax = 5.0
    if df['G_Z'][0] > xmax:
        xmax = df['G_Z'][0] + 0.5
    if df['Z_W1'][0] > ymax:
        ymax = df['Z_W1'][0] + 0.5
    xmin = -0.5
    ymin = 0.5
    if df['G_Z'][0] < xmin:
        xmin = df['G_Z'][0] - 0.5
    if df['Z_W1'][0] < ymin:
        ymin = df['Z_W1'][0] - 0.5
    
    ax.set_title(fi, **font)
    ax.set_xlabel(r'$g-z$', **font)
    ax.set_ylabel(r'$z-W1$', **font)
    ax.set_xlim(xmin, xmax)
    ax.set_ylim(ymin, ymax)
    ax.tick_params(axis='both', which='both', labelsize='18')
    #plt.gcf().subplots_adjust(bottom=0.15)
    plt.savefig(folder+ '/' + fi + '_gz-zw1' + ex, bbox_inches='tight', dpi=300)

def PlotSpreadVsMag(folder, df, fi, ex):
    ot = ''
    if np.abs(df['SPREAD_MODEL_I'][0]) <= 0.003:
        ot = 'Pointlike'
    else:
        ot = 'Extended'
    
    fig = pickle.load(open('easyweb/static/plotdata/spreadmag_plot.pkl','rb'))
    ax = fig.add_subplot(111)
    ax.scatter(df['MAG_I'][0], df['SPREAD_MODEL_I'][0], color='maroon', alpha=0.75, s=100, zorder=10)
    
    #ax.text(21.0, 0.035, 'Suggested Object\nType: ' + ot, va='center', **font3)
    ax.text(0.63, 0.92, 'Suggested Object\nType: ' + ot, va='center', transform=ax.transAxes, **font3)
    
    ax.set_title(fi, **font)
    ax.set_xlabel(r'$i\/\/mag$', **font)
    ax.set_ylabel(r'$i\/\/spread$', **font)
    #ax.set_xlim(17,21)
    #ax.set_ylim(-0.015,0.045)
    ax.tick_params(axis='both', which='both', labelsize='18')
    plt.gcf().subplots_adjust(bottom=0.15, left=0.15)
    plt.savefig(folder+ '/' + fi + '_spreadmag' + ex, bbox_inches='tight', dpi=300)
    
def WCStoPixels(header,RA,DEC):
    w = WCS(header)
    px, py = w.wcs_world2pix(RA, DEC, 1)
    px = np.round(px, 1)
    py = np.round(py, 1)
    return(px, py)

def PixelstoWCS(header,x,y):
    w = WCS(header)
    wx, wy = w.wcs_pix2world(x, y, 1)
    return(wx, wy)

def CreateChart(image, header, data, xs, ys, makePlot, helperPlot, USERObject, df, filenm, band):
    fig = plt.figure()
    ax = fig.add_subplot(111, projection=WCS(image))
    
    ax.imshow(data, origin='lower', cmap='gray_r', extent=[1,header['NAXIS1'],1,header['NAXIS2']])
    
    ra = ax.coords[0]
    ra.set_ticklabel_position('b')
    ra.set_major_formatter('hh:mm:ss')
    ra.set_ticks(exclude_overlapping=True)
    
    dec = ax.coords[1]
    dec.set_ticklabel_position('l')
    dec.set_major_formatter('dd:mm:ss')
    
    if makePlot is True:
        # DRAW BOXES AROUND USER OBJECT
        ax.add_patch(Rectangle((header['CRPIX1']-9, header['CRPIX2']-9), width=18, height=18, edgecolor='blue', facecolor='none', linewidth=1))
    
        if helperPlot is True:
            if df['COADD_OBJECT_ID'][0] != USERObject['COADD_OBJECT_ID'][0]:
                # DRAW BOX AROUND HELPER OBJECT
                x, y = WCStoPixels(header, df['RA'][0], df['DEC'][0])        # Pixel coordinates of the helper object
                ax.add_patch(Rectangle((x-11, y-11), width=22, height=22, edgecolor='blue', facecolor='none', linewidth=1))
                
                # FIGURES OUT WHERE THE HELPER OBJECT IS IN RELATION TO THE USER OBJECT
                XislessthanRef = False        # True if the returned x-coord is less then the reference x-coord
                pxdist = x - header['CRPIX1']
                if pxdist < 0:
                    XislessthanRef = True
                else:
                    XislessthanRef = False
                
                YislessthanRef = False        # True if the returned y-coord is less then the reference y-coord
                pydist = y - header['CRPIX2']
                if pydist < 0:
                    YislessthanRef = True
                else:
                    YislessthanRef = False
                pdist = np.round(np.sqrt(pxdist**2 + pydist**2), 2)
                theta = np.rad2deg(np.arctan(pydist/pxdist))
                #theta = np.rad2deg(np.arctan2(pydist,pxdist))
                wxdist = np.abs(df['RA'][0] - header['CRVAL1'])/ARCSEC_TO_DEG
                wydist = np.abs(df['DEC'][0] - header['CRVAL2'])/ARCSEC_TO_DEG
        
                # DRAW ARROWS BETWEEN BOXES, LABEL DISTANCES
                boxstyle = {'facecolor':'grey','edgecolor':'None','alpha':0.75,'pad':1.0}
                ax.annotate('', xy=(x, header['CRPIX2']), xytext=(header['CRPIX1'], header['CRPIX2']), arrowprops=dict(arrowstyle='<-', color='blue'), zorder=5)
                if XislessthanRef == False:
                    if YislessthanRef == True:
                        ax.text(header['CRPIX1']+(pxdist/2), header['CRPIX1']+(5*float(ys))-(110*(int(xs)-int(ys))), '+'+str(np.abs(pxdist))+'\"', ha='center', va='center', color='blue', zorder=10, bbox=boxstyle)
                    else:
                        ax.text(header['CRPIX1']+(pxdist/2), header['CRPIX1']-(5*float(ys))-(5*float(xs))-(110*(int(xs)-int(ys))), '+'+str(np.abs(pxdist))+'\"', ha='center', va='center', color='blue', zorder=10, bbox=boxstyle)
                else:
                    if YislessthanRef == True:
                        ax.text(header['CRPIX1']+(pxdist/2), header['CRPIX1']+(5*float(ys))-(110*(int(xs)-int(ys))), '-'+str(np.abs(pxdist))+'\"', ha='center', va='center', color='blue', zorder=10, bbox=boxstyle)
                    else:
                        ax.text(header['CRPIX1']+(pxdist/2), header['CRPIX1']-(5*float(ys))-(5*float(xs))-(110*(int(xs)-int(ys))), '-'+str(np.abs(pxdist))+'\"', ha='center', va='center', color='blue', zorder=10, bbox=boxstyle)
                
                ax.annotate('', xy=(x, y), xytext=(x, header['CRPIX2']), arrowprops=dict(arrowstyle='<-', color='blue'), zorder=5)
                if YislessthanRef == True:
                    if XislessthanRef == True:
                        ax.text(x-(10*float(xs)), header['CRPIX2']+(pydist/2), '-'+str(np.abs(pydist))+'\"', ha='center', va='center', rotation=270, color='blue', zorder=10, bbox=boxstyle)
                    else:
                        ax.text(x+(10*float(xs)), header['CRPIX2']+(pydist/2), '-'+str(np.abs(pydist))+'\"', ha='center', va='center', rotation=270, color='blue', zorder=10, bbox=boxstyle)
                else:
                    if XislessthanRef == True:
                        ax.text(x-(10*float(xs)), header['CRPIX2']+(pydist/2), '+'+str(np.abs(pydist))+'\"', ha='center', va='center', rotation=270, color='blue', zorder=10, bbox=boxstyle)
                    else:
                        ax.text(x+(10*float(xs)), header['CRPIX2']+(pydist/2), '+'+str(np.abs(pydist))+'\"', ha='center', va='center', rotation=270, color='blue', zorder=10, bbox=boxstyle)
                
                ax.annotate('', xy=(x, y), xytext=(header['CRPIX1'], header['CRPIX2']), arrowprops=dict(arrowstyle='<-', color='blue'), zorder=5)
    
                # PLOT THE TEXT FOR THE HYPOTENUSE ARROW
                if XislessthanRef == False and YislessthanRef == False:
                    ax.text(header['CRPIX1']+(pxdist/2)-(6*float(xs)), header['CRPIX2']+(pydist/2)+(6*float(ys)), str(np.abs(pdist))+'\"', ha='center', va='center', rotation=theta, color='blue', zorder=10, bbox=boxstyle)
                elif XislessthanRef == True and YislessthanRef == False:
                    ax.text(header['CRPIX1']+(pxdist/2)+(6*float(xs)), header['CRPIX2']+(pydist/2)+(6*float(ys)), str(np.abs(pdist))+'\"', ha='center', va='center', rotation=theta, color='blue', zorder=10, bbox=boxstyle)
                elif XislessthanRef == False and YislessthanRef == True:
                    ax.text(header['CRPIX1']+(pxdist/2)-(10*float(xs)), header['CRPIX2']+(pydist/2)-(10*float(ys)), str(np.abs(pdist))+'\"', ha='center', va='center', rotation=theta, color='blue', zorder=10, bbox=boxstyle)
                else:
                    ax.text(header['CRPIX1']+(pxdist/2)+(6*float(xs)), header['CRPIX2']+(pydist/2)-(6*float(ys)), str(np.abs(pdist))+'\"', ha='center', va='center', rotation=theta, color='blue', zorder=10, bbox=boxstyle)
            elif df['COADD_OBJECT_ID'][0] == USERObject['COADD_OBJECT_ID'][0]:
                logfile.write('The object you wish to find is the brightest object in this field within the magnitude threshold you have selected.\n')
    
    ax.grid(ls=':', lw=0.5, color='black', zorder=3)
    ax.set_axisbelow(True)
    ax.set_title(filenm + '_' + band, **font)
    ax.set_xlabel('RA', **font)
    ax.set_ylabel('DEC', **font)
    
    return(ax)
